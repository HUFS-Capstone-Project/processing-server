from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from app.core.config import Settings
from app.domain.crawl import crawl_and_parse
from app.domain.job import JobRecord

logger = logging.getLogger("processing.worker.processor")


@dataclass(slots=True)
class JobProcessOutcome:
    processed: bool
    succeeded: bool
    timed_out: bool
    elapsed_ms: int


class JobRepositoryPort(Protocol):
    async def claim_job(self, job_id: UUID) -> JobRecord | None: ...

    async def upsert_job_result(self, **kwargs): ...

    async def mark_succeeded(self, job_id: UUID): ...

    async def mark_failed(self, job_id: UUID, error_message: str): ...


class JobProcessor:
    def __init__(
        self,
        *,
        repository: JobRepositoryPort,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._settings = settings

    async def process_job(self, job_id: UUID) -> JobProcessOutcome:
        started = time.monotonic()
        job = await self._repository.claim_job(job_id)
        if not job:
            logger.info("job skipped (not found or already claimed) job_id=%s", job_id)
            return JobProcessOutcome(
                processed=False,
                succeeded=False,
                timed_out=False,
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        logger.info("job claimed job_id=%s source_url=%s", job.job_id, job.source_url)

        try:
            crawl_artifact = await crawl_and_parse(job.source_url, self._settings)
            # TODO(ner): Add embedding-based extraction in next migration step.
            # TODO(kakao): Add Kakao Local enrichment and final place ranking in next migration step.
            logger.info(
                "job crawl completed job_id=%s caption_len=%s",
                job.job_id,
                len(crawl_artifact.caption or ""),
            )

            await self._repository.upsert_job_result(
                job_id=job.job_id,
                caption=crawl_artifact.caption,
                instagram_meta=crawl_artifact.instagram_meta,
            )
            await self._repository.mark_succeeded(job.job_id)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info("job succeeded job_id=%s elapsed_ms=%s", job.job_id, elapsed_ms)
            return JobProcessOutcome(
                processed=True,
                succeeded=True,
                timed_out=False,
                elapsed_ms=elapsed_ms,
            )
        except Exception as exc:
            logger.exception("job processing failed job_id=%s", job_id)
            await self._repository.mark_failed(job_id, f"{exc.__class__.__name__}: {exc}")
            elapsed_ms = int((time.monotonic() - started) * 1000)
            timed_out = isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or (
                exc.__class__.__name__ == "PlaywrightTimeoutError"
            )
            return JobProcessOutcome(
                processed=True,
                succeeded=False,
                timed_out=timed_out,
                elapsed_ms=elapsed_ms,
            )
