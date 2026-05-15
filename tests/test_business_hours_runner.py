from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

from app.worker.business_hours_processor import BusinessHoursProcessOutcome
from app.worker.business_hours_runner import BusinessHoursWorkerRunner


def _run(coro):
    return asyncio.run(coro)


class FakeBusinessHoursQueue:
    def __init__(self, job_ids: list[UUID]) -> None:
        self.job_ids = list(job_ids)
        self.dequeued: list[UUID] = []
        self.acked: list[UUID] = []
        self.recover_calls = 0
        self.promote_calls = 0

    async def dequeue(self, timeout_seconds: int) -> UUID | None:
        if not self.job_ids:
            return None
        job_id = self.job_ids.pop(0)
        self.dequeued.append(job_id)
        return job_id

    async def ack(self, job_id: UUID) -> None:
        self.acked.append(job_id)

    async def recover_stale_processing_jobs(self, stale_after_seconds: int) -> int:
        self.recover_calls += 1
        return 0

    async def promote_due_jobs(self, batch_size: int) -> int:
        self.promote_calls += 1
        return 0


class BlockingBusinessHoursWorker:
    def __init__(self) -> None:
        self.started: list[UUID] = []
        self.release = asyncio.Event()

    async def process_job(self, job_id: UUID) -> BusinessHoursProcessOutcome:
        self.started.append(job_id)
        await self.release.wait()
        return BusinessHoursProcessOutcome(
            processed=True,
            succeeded=True,
            elapsed_ms=1,
            queue_wait_ms=2,
            total_elapsed_ms=3,
            kakao_place_id=str(job_id),
            status="SUCCEEDED",
            error_code=None,
        )


class FakeBusinessHoursRepository:
    def __init__(self) -> None:
        self.recover_calls = 0

    async def recover_stale_processing_jobs(self, stale_after_seconds: int) -> list[UUID]:
        self.recover_calls += 1
        return []


def test_business_hours_runner_concurrency_one_dequeues_one_at_a_time() -> None:
    async def scenario():
        job_ids = [uuid4(), uuid4()]
        queue = FakeBusinessHoursQueue(job_ids)
        worker = BlockingBusinessHoursWorker()
        runner = BusinessHoursWorkerRunner(
            queue=queue,
            worker=worker,
            concurrency=1,
            pop_timeout_seconds=0,
            idle_sleep_seconds=0,
            stale_timeout_seconds=60,
            promote_batch_size=10,
        )

        await runner.run_once()
        await asyncio.sleep(0)
        await runner.run_once()

        assert queue.dequeued == [job_ids[0]]
        assert worker.started == [job_ids[0]]

        worker.release.set()
        await runner.drain()
        assert queue.acked == [job_ids[0]]

    _run(scenario())


def test_business_hours_runner_concurrency_n_dequeues_at_most_n_jobs() -> None:
    async def scenario():
        job_ids = [uuid4(), uuid4(), uuid4()]
        queue = FakeBusinessHoursQueue(job_ids)
        worker = BlockingBusinessHoursWorker()
        runner = BusinessHoursWorkerRunner(
            queue=queue,
            worker=worker,
            concurrency=2,
            pop_timeout_seconds=0,
            idle_sleep_seconds=0,
            stale_timeout_seconds=60,
            promote_batch_size=10,
        )

        await runner.run_once()
        await asyncio.sleep(0)
        await runner.run_once()

        assert queue.dequeued == job_ids[:2]
        assert worker.started == job_ids[:2]
        assert queue.job_ids == job_ids[2:]

        worker.release.set()
        await runner.drain()
        assert queue.acked == job_ids[:2]

    _run(scenario())


def test_business_hours_runner_runs_db_and_queue_stale_recovery() -> None:
    async def scenario():
        queue = FakeBusinessHoursQueue([])
        worker = BlockingBusinessHoursWorker()
        repository = FakeBusinessHoursRepository()
        runner = BusinessHoursWorkerRunner(
            queue=queue,
            worker=worker,
            repository=repository,
            concurrency=1,
            pop_timeout_seconds=0,
            idle_sleep_seconds=0,
            stale_timeout_seconds=60,
            promote_batch_size=10,
        )

        await runner.run_once()

        assert repository.recover_calls == 1
        assert queue.recover_calls == 1
        assert queue.promote_calls == 1

    _run(scenario())
