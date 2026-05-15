from __future__ import annotations

import asyncio
import logging
from typing import Protocol
from uuid import UUID

from app.core.config import get_settings
from app.infra.db import BusinessHoursRepository, create_db_pool
from app.infra.queue import RedisJobQueue
from app.worker.business_hours_processor import BusinessHoursProcessOutcome, BusinessHoursWorker

logger = logging.getLogger("processing.business_hours.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class BusinessHoursQueuePort(Protocol):
    async def dequeue(self, timeout_seconds: int) -> UUID | None: ...

    async def ack(self, job_id: UUID) -> None: ...

    async def recover_stale_processing_jobs(self, stale_after_seconds: int) -> int: ...

    async def promote_due_jobs(self, batch_size: int) -> int: ...


class BusinessHoursWorkerPort(Protocol):
    async def process_job(self, job_id: UUID) -> BusinessHoursProcessOutcome: ...


class BusinessHoursRecoveryPort(Protocol):
    async def recover_stale_processing_jobs(self, stale_after_seconds: int) -> list[UUID]: ...


class BusinessHoursWorkerRunner:
    def __init__(
        self,
        *,
        queue: BusinessHoursQueuePort,
        worker: BusinessHoursWorkerPort,
        repository: BusinessHoursRecoveryPort | None = None,
        concurrency: int,
        pop_timeout_seconds: int,
        idle_sleep_seconds: float,
        stale_timeout_seconds: int,
        promote_batch_size: int,
    ) -> None:
        self._queue = queue
        self._worker = worker
        self._repository = repository
        self._concurrency = max(1, concurrency)
        self._pop_timeout_seconds = pop_timeout_seconds
        self._idle_sleep_seconds = idle_sleep_seconds
        self._stale_timeout_seconds = stale_timeout_seconds
        self._promote_batch_size = promote_batch_size
        self._running: set[asyncio.Task] = set()

    @property
    def running_count(self) -> int:
        self._prune_finished()
        return len(self._running)

    async def run_forever(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception:
                logger.exception("business hours worker loop error")
                await asyncio.sleep(self._idle_sleep_seconds)

    async def drain(self) -> None:
        if self._running:
            await asyncio.gather(*self._running, return_exceptions=True)

    async def run_once(self) -> None:
        self._prune_finished()
        if len(self._running) >= self._concurrency:
            await self._wait_for_capacity()
            return

        if self._repository is not None:
            recovered_ids = await self._repository.recover_stale_processing_jobs(self._stale_timeout_seconds)
            if recovered_ids:
                logger.warning(
                    "business hours db stale recovery recovered=%s stale_after_seconds=%s",
                    len(recovered_ids),
                    self._stale_timeout_seconds,
                )
        await self._queue.recover_stale_processing_jobs(self._stale_timeout_seconds)
        await self._queue.promote_due_jobs(self._promote_batch_size)

        capacity = self._concurrency - len(self._running)
        for index in range(capacity):
            timeout = self._pop_timeout_seconds if not self._running and index == 0 else 0
            job_id = await self._queue.dequeue(timeout)
            if not job_id:
                if not self._running:
                    await asyncio.sleep(self._idle_sleep_seconds)
                break
            self._running.add(asyncio.create_task(self._process_queued_job(job_id)))

    async def _process_queued_job(self, job_id: UUID) -> None:
        outcome = await self._worker.process_job(job_id)
        if outcome.processed:
            await self._queue.ack(job_id)
        logger.info(
            (
                "business hours processed job_id=%s kakao_place_id=%s status=%s detail_status=%s "
                "daily_hours_count=%s error_code=%s queue_wait_ms=%s processing_elapsed_ms=%s "
                "total_elapsed_ms=%s processed=%s succeeded=%s"
            ),
            job_id,
            outcome.kakao_place_id,
            outcome.status,
            outcome.detail_status,
            outcome.daily_hours_count,
            outcome.error_code,
            outcome.queue_wait_ms,
            outcome.elapsed_ms,
            outcome.total_elapsed_ms,
            outcome.processed,
            outcome.succeeded,
        )

    def _prune_finished(self) -> None:
        self._running = {task for task in self._running if not task.done()}

    async def _wait_for_capacity(self) -> None:
        if not self._running:
            return
        done, pending = await asyncio.wait(
            self._running,
            timeout=self._idle_sleep_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            try:
                task.result()
            except Exception:
                logger.exception("business hours worker task failed")
        self._running = set(pending)


async def run_business_hours_worker() -> None:
    settings = get_settings()
    pool = await create_db_pool(settings)
    queue = RedisJobQueue.from_business_hours_settings(settings)
    repository = BusinessHoursRepository(pool, settings.processing_schema)
    worker = BusinessHoursWorker(repository=repository, settings=settings)
    runner = BusinessHoursWorkerRunner(
        queue=queue,
        worker=worker,
        repository=repository,
        concurrency=settings.business_hours_worker_concurrency,
        pop_timeout_seconds=settings.business_hours_queue_pop_timeout_seconds,
        idle_sleep_seconds=settings.business_hours_worker_idle_sleep_seconds,
        stale_timeout_seconds=settings.business_hours_fetching_stale_timeout_seconds,
        promote_batch_size=settings.queue_promote_batch_size,
    )

    logger.info(
        "business hours worker started concurrency=%s queue_namespace=%s",
        settings.business_hours_worker_concurrency,
        settings.business_hours_queue_namespace,
    )
    try:
        await runner.run_forever()
    finally:
        await runner.drain()
        await queue.close()
        await pool.close()


def main() -> None:
    asyncio.run(run_business_hours_worker())


if __name__ == "__main__":
    main()
