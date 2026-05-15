from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

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

    async def create_job(
        self,
        *,
        job_id: UUID,
        room_id: UUID,
        source_url: str,
    ) -> JobRecord:
        now = datetime.now(timezone.utc)
        job = JobRecord(
            job_id=job_id,
            room_id=room_id,
            source_url=source_url,
            status=JobStatus.QUEUED,
            error_message=None,
            created_at=now,
            updated_at=now,
        )
        self._jobs_by_id[job_id] = job
        return job

    async def get_job(self, job_id: UUID) -> JobRecord | None:
        return self._jobs_by_id.get(job_id)

    async def get_job_result(self, job_id: UUID):
        return None

    async def mark_job_enqueue_failed(self, job_id: UUID, error_message: str) -> None:
        job = self._jobs_by_id[job_id]
        self._jobs_by_id[job_id] = JobRecord(
            job_id=job.job_id,
            room_id=job.room_id,
            source_url=job.source_url,
            status=JobStatus.FAILED,
            error_message=error_message,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_create_job_enqueues_once() -> None:
    repo = FakeRepository()
    queue = FakeQueue(enqueued=[])
    service = JobService(repo, queue)

    command = CreateJobCommand(
        url="https://www.instagram.com/reel/abcde/",
        room_id=uuid4(),
    )

    job = _run(service.create_job(command))

    assert queue.enqueued == [job.job_id]
    assert job.room_id == command.room_id
    assert job.status == JobStatus.QUEUED


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_create_job_reuses_duplicate_without_enqueueing_again() -> None:
    repo = FakeRepository()
    queue = FakeQueue(enqueued=[])
    service = JobService(repo, queue)
    room_id = uuid4()
    command = CreateJobCommand(
        url="https://www.instagram.com/reel/abcde/",
        room_id=room_id,
    )

    first = _run(service.create_job(command))

    async def create_existing_job(*, job_id, room_id, source_url):
        return first

    repo.create_job = create_existing_job
    second = _run(service.create_job(command))

    assert second.job_id == first.job_id
    assert queue.enqueued == [first.job_id]


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_create_job_rejects_invalid_url() -> None:
    repo = FakeRepository()
    queue = FakeQueue(enqueued=[])
    service = JobService(repo, queue)

    with pytest.raises(InvalidJobRequest):
        _run(service.create_job(CreateJobCommand(url="ftp://invalid.example.com", room_id=uuid4())))
