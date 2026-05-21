from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse
from uuid import UUID, uuid4

from app.domain.job.model import JobRecord, JobResultRecord
from app.domain.url_contract import is_instagram_media_url

INSTAGRAM_RATE_LIMITED_ERROR_CODE = "INSTAGRAM_RATE_LIMITED"


class JobRepositoryPort(Protocol):
    async def create_job(
        self,
        *,
        job_id: UUID,
        room_id: UUID,
        original_url: str,
    ) -> JobRecord: ...

    async def get_job(self, job_id: UUID) -> JobRecord | None: ...

    async def get_job_result(self, job_id: UUID) -> JobResultRecord | None: ...

    async def mark_job_enqueue_failed(self, job_id: UUID, error_message: str) -> None: ...


class JobQueuePort(Protocol):
    async def enqueue(self, job_id: UUID) -> None: ...


class InstagramCooldownPort(Protocol):
    async def instagram_cooldown_ttl(self) -> int: ...


@dataclass(slots=True)
class CreateJobCommand:
    original_url: str
    room_id: UUID


class InvalidJobRequest(Exception):
    pass


class InstagramRateLimited(Exception):
    def __init__(self, cooldown_seconds: int) -> None:
        self.cooldown_seconds = max(0, int(cooldown_seconds))
        super().__init__(
            f"Instagram crawling is temporarily rate-limited. Retry after {self.cooldown_seconds} seconds."
        )


class JobService:
    def __init__(
        self,
        repository: JobRepositoryPort,
        queue: JobQueuePort,
        cooldown_store: InstagramCooldownPort | None = None,
    ) -> None:
        self._repository = repository
        self._queue = queue
        self._cooldown_store = cooldown_store

    async def create_job(self, command: CreateJobCommand) -> JobRecord:
        original_url = self._validate_url(command.original_url)
        if self._cooldown_store and is_instagram_media_url(original_url):
            cooldown_seconds = await self._cooldown_store.instagram_cooldown_ttl()
            if cooldown_seconds > 0:
                raise InstagramRateLimited(cooldown_seconds)
        job_id = uuid4()
        job = await self._repository.create_job(
            job_id=job_id,
            room_id=command.room_id,
            original_url=original_url,
        )
        if job.job_id != job_id:
            return job

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


