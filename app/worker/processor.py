from __future__ import annotations

import math
from typing import Protocol
from uuid import UUID

from app.core.config import Settings
from app.domain.crawl import crawl_and_parse
from app.domain.job import JobRecord


class JobRepositoryPort(Protocol):
    async def claim_job(self, job_id: UUID) -> JobRecord | None: ...

    async def upsert_job_result(self, **kwargs): ...

    async def mark_succeeded(self, job_id: UUID): ...

    async def mark_for_retry(self, job_id: UUID, error_code: str, error_message: str, delay_seconds: int): ...

    async def mark_failed(self, job_id: UUID, error_code: str, error_message: str): ...


class DelayQueuePort(Protocol):
    async def enqueue_delayed(self, job_id: UUID, delay_seconds: int) -> None: ...


class JobProcessor:
    def __init__(
        self,
        *,
        repository: JobRepositoryPort,
        queue: DelayQueuePort,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._queue = queue
        self._settings = settings

    async def process_job(self, job_id: UUID) -> None:
        job = await self._repository.claim_job(job_id)
        if not job:
            return

        try:
            crawl_artifact = await crawl_and_parse(job.source_url, self._settings)
            # TODO(ner): Replace crawl-only flow with embedding-based NER/candidate extraction.
            # TODO(ner): Persist extracted candidates in `raw_candidates` with sentence-level evidence.
            # TODO(kakao): Query Kakao Local API with extracted candidates and store ranked `places`.

            await self._repository.upsert_job_result(
                job_id=job.job_id,
                media_type=crawl_artifact.media_type,
                caption=crawl_artifact.caption,
                instagram_meta=crawl_artifact.instagram_meta,
                raw_candidates=[],
                places=[],
                kakao_raw={},
            )
            await self._repository.mark_succeeded(job.job_id)
        except Exception as exc:
            if self._is_retryable_exception(exc):
                await self._retry_or_fail(job_id, job.attempt, job.max_attempts, "PROCESSING_RETRYABLE_ERROR", str(exc))
            else:
                await self._repository.mark_failed(job_id, "PROCESSING_FATAL_ERROR", str(exc))

    async def _retry_or_fail(
        self,
        job_id: UUID,
        current_attempt: int,
        max_attempts: int,
        error_code: str,
        error_message: str,
    ) -> None:
        if current_attempt >= max_attempts:
            await self._repository.mark_failed(job_id, error_code, error_message)
            return

        delay_seconds = self._compute_backoff_seconds(current_attempt)
        await self._repository.mark_for_retry(job_id, error_code, error_message, delay_seconds)
        await self._queue.enqueue_delayed(job_id, delay_seconds)

    def _compute_backoff_seconds(self, current_attempt: int) -> int:
        base = max(1, self._settings.worker_retry_base_seconds)
        max_delay = max(base, self._settings.worker_retry_max_seconds)
        delay = base * math.pow(2, max(0, current_attempt - 1))
        return int(min(max_delay, max(base, delay)))

    @staticmethod
    def _is_retryable_exception(exc: Exception) -> bool:
        message = str(exc).lower()
        retryable_tokens = [
            "timeout",
            "temporar",
            "connection",
            "econnreset",
            "dns",
            "429",
            "502",
            "503",
            "504",
        ]
        return any(token in message for token in retryable_tokens)
