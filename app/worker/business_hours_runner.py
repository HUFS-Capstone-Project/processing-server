from __future__ import annotations

import asyncio
import logging

from app.core.config import get_settings
from app.infra.db import BusinessHoursRepository, create_db_pool
from app.infra.queue import RedisJobQueue
from app.worker.business_hours_processor import BusinessHoursWorker

logger = logging.getLogger("processing.business_hours.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


async def run_business_hours_worker() -> None:
    settings = get_settings()
    pool = await create_db_pool(settings)
    queue = RedisJobQueue.from_business_hours_settings(settings)
    repository = BusinessHoursRepository(pool, settings.processing_schema)
    worker = BusinessHoursWorker(repository=repository, settings=settings)
    semaphore = asyncio.Semaphore(max(1, settings.business_hours_worker_concurrency))
    running: set[asyncio.Task] = set()

    async def process_queued_job(job_id) -> None:
        async with semaphore:
            outcome = await worker.process_job(job_id)
        if outcome.processed:
            await queue.ack(job_id)
        logger.info(
            "business hours processed job_id=%s processed=%s succeeded=%s elapsed_ms=%s",
            job_id,
            outcome.processed,
            outcome.succeeded,
            outcome.elapsed_ms,
        )

    logger.info(
        "business hours worker started concurrency=%s queue_namespace=%s",
        settings.business_hours_worker_concurrency,
        settings.business_hours_queue_namespace,
    )
    try:
        while True:
            try:
                running = {task for task in running if not task.done()}
                await queue.recover_stale_processing_jobs(settings.business_hours_fetching_stale_timeout_seconds)
                await queue.promote_due_jobs(settings.queue_promote_batch_size)
                job_id = await queue.dequeue(settings.business_hours_queue_pop_timeout_seconds)
                if not job_id:
                    await asyncio.sleep(settings.business_hours_worker_idle_sleep_seconds)
                    continue

                task = asyncio.create_task(process_queued_job(job_id))
                running.add(task)
            except Exception:
                logger.exception("business hours worker loop error")
                await asyncio.sleep(settings.business_hours_worker_idle_sleep_seconds)
    finally:
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        await queue.close()
        await pool.close()


def main() -> None:
    asyncio.run(run_business_hours_worker())


if __name__ == "__main__":
    main()
