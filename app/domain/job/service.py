from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse
from uuid import UUID, uuid4

from app.domain.job.model import JobRecord, JobResultRecord


class JobRepositoryPort(Protocol):
    async def create_job(
        self,
        *,
        job_id: UUID,
        source_url: str,
        source_url_hash: str,
        idempotency_key: str | None,
        source: str | None,
        room_id: str | None,
        max_attempts: int,
    ) -> tuple[JobRecord, bool]: ...

    async def get_job(self, job_id: UUID) -> JobRecord | None: ...

    async def get_job_result(self, job_id: UUID) -> JobResultRecord | None: ...

    async def mark_job_enqueue_failed(self, job_id: UUID, error_message: str) -> None: ...


class JobQueuePort(Protocol):
    async def enqueue(self, job_id: UUID) -> None: ...


@dataclass(slots=True)
class CreateJobCommand:
    url: str
    idempotency_key: str | None = None
    source: str | None = None
    room_id: str | None = None


class InvalidJobRequest(Exception):
    pass


class JobService:
    def __init__(
        self,
        repository: JobRepositoryPort,
        queue: JobQueuePort,
        *,
        max_attempts: int,
    ) -> None:
        self._repository = repository
        self._queue = queue
        self._max_attempts = max_attempts

    async def create_job(self, command: CreateJobCommand) -> tuple[JobRecord, bool]:
        normalized_url = self._validate_url(command.url)
        source_url_hash = hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()

        job_id = uuid4()

        job, created = await self._repository.create_job(
            job_id=job_id,
            source_url=normalized_url,
            source_url_hash=source_url_hash,
            idempotency_key=command.idempotency_key,
            source=command.source,
            room_id=command.room_id,
            max_attempts=self._max_attempts,
        )

        if created:
            try:
                await self._queue.enqueue(job.job_id)
            except Exception as exc:
                await self._repository.mark_job_enqueue_failed(job.job_id, str(exc))
                raise

        return job, created

    async def get_job(self, job_id: UUID) -> JobRecord | None:
        return await self._repository.get_job(job_id)

    async def get_job_result(self, job_id: UUID) -> JobResultRecord | None:
        return await self._repository.get_job_result(job_id)

    @staticmethod
    def _validate_url(url: str) -> str:
        raw = (url or "").strip()
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"}:
            raise InvalidJobRequest("Only http/https URLs are supported.")
        if not parsed.netloc:
            raise InvalidJobRequest("URL host is required.")
        return raw


