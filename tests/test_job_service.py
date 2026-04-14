from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

import pytest

from app.domain.job import JobRecord, JobService, JobStatus
from app.domain.job.service import CreateJobCommand, InvalidJobRequest

if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _can_create_event_loop() -> bool:
    try:
        loop = asyncio.new_event_loop()
        loop.close()
        return True
    except OSError:
        return False


EVENT_LOOP_AVAILABLE = _can_create_event_loop()


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


@dataclass
class FakeQueue:
    enqueued: list[UUID]

    async def enqueue(self, job_id: UUID) -> None:
        self.enqueued.append(job_id)


class FakeRepository:
    def __init__(self) -> None:
        self._jobs_by_id: dict[UUID, JobRecord] = {}
        self._jobs_by_idempotency: dict[str, JobRecord] = {}

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
    ) -> tuple[JobRecord, bool]:
        if idempotency_key and idempotency_key in self._jobs_by_idempotency:
            return self._jobs_by_idempotency[idempotency_key], False

        now = datetime.now(timezone.utc)
        job = JobRecord(
            job_id=job_id,
            source_url=source_url,
            source_url_hash=source_url_hash,
            status=JobStatus.QUEUED,
            attempt=0,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            source=source,
            room_id=room_id,
            error_code=None,
            error_message=None,
            queued_at=now,
            processing_started_at=None,
            completed_at=None,
            next_retry_at=None,
            created_at=now,
            updated_at=now,
        )
        self._jobs_by_id[job_id] = job
        if idempotency_key:
            self._jobs_by_idempotency[idempotency_key] = job
        return job, True

    async def get_job(self, job_id: UUID) -> JobRecord | None:
        return self._jobs_by_id.get(job_id)

    async def get_job_result(self, job_id: UUID):
        return None

    async def mark_job_enqueue_failed(self, job_id: UUID, error_message: str) -> None:
        job = self._jobs_by_id[job_id]
        self._jobs_by_id[job_id] = JobRecord(
            job_id=job.job_id,
            source_url=job.source_url,
            source_url_hash=job.source_url_hash,
            status=JobStatus.FAILED,
            attempt=job.attempt,
            max_attempts=job.max_attempts,
            idempotency_key=job.idempotency_key,
            source=job.source,
            room_id=job.room_id,
            error_code="QUEUE_ENQUEUE_FAILED",
            error_message=error_message,
            queued_at=job.queued_at,
            processing_started_at=job.processing_started_at,
            completed_at=job.completed_at,
            next_retry_at=job.next_retry_at,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_create_job_with_idempotency_reuses_job() -> None:
    repo = FakeRepository()
    queue = FakeQueue(enqueued=[])
    service = JobService(repo, queue, max_attempts=3)

    command = CreateJobCommand(
        url="https://www.instagram.com/reel/abcde/",
        idempotency_key="idem-1",
        source="web",
        room_id="room-123",
    )

    first_job, first_created = _run(service.create_job(command))
    second_job, second_created = _run(service.create_job(command))

    assert first_created is True
    assert second_created is False
    assert first_job.job_id == second_job.job_id
    assert queue.enqueued == [first_job.job_id]


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_create_job_rejects_invalid_url() -> None:
    repo = FakeRepository()
    queue = FakeQueue(enqueued=[])
    service = JobService(repo, queue, max_attempts=3)

    with pytest.raises(InvalidJobRequest):
        _run(service.create_job(CreateJobCommand(url="ftp://invalid.example.com")))
