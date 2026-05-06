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

    logger.info(
        "business hours worker started concurrency=%s queue_namespace=%s",
        settings.business_hours_worker_concurrency,
        settings.business_hours_queue_namespace,
    )
    try:
        while True:
            try:
                job_id = await queue.dequeue(settings.business_hours_queue_pop_timeout_seconds)
                if not job_id:
                    await asyncio.sleep(settings.business_hours_worker_idle_sleep_seconds)
                    continue

                async with semaphore:
                    outcome = await worker.process_job(job_id)
                logger.info(
                    "business hours processed job_id=%s processed=%s succeeded=%s elapsed_ms=%s",
                    job_id,
                    outcome.processed,
                    outcome.succeeded,
                    outcome.elapsed_ms,
                )
            except Exception:
                logger.exception("business hours worker loop error")
                await asyncio.sleep(settings.business_hours_worker_idle_sleep_seconds)
    finally:
        await queue.close()
        await pool.close()


def main() -> None:
    asyncio.run(run_business_hours_worker())


if __name__ == "__main__":
    main()
