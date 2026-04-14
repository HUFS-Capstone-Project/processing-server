from __future__ import annotations

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
        room_id: UUID,
        source_url: str,
    ) -> JobRecord: ...

    async def get_job(self, job_id: UUID) -> JobRecord | None: ...

    async def get_job_result(self, job_id: UUID) -> JobResultRecord | None: ...

    async def mark_job_enqueue_failed(self, job_id: UUID, error_message: str) -> None: ...


class JobQueuePort(Protocol):
    async def enqueue(self, job_id: UUID) -> None: ...


@dataclass(slots=True)
class CreateJobCommand:
    url: str
    room_id: UUID


class InvalidJobRequest(Exception):
    pass


class JobService:
    def __init__(
        self,
        repository: JobRepositoryPort,
        queue: JobQueuePort,
    ) -> None:
        self._repository = repository
        self._queue = queue

    async def create_job(self, command: CreateJobCommand) -> JobRecord:
        normalized_url = self._validate_url(command.url)
        job = await self._repository.create_job(
            job_id=uuid4(),
            room_id=command.room_id,
            source_url=normalized_url,
        )

        try:
            await self._queue.enqueue(job.job_id)
        except Exception as exc:
            await self._repository.mark_job_enqueue_failed(job.job_id, str(exc))
            raise

        return job

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


