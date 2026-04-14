from __future__ import annotations

import asyncio
import logging
import time

from app.core.config import get_settings
from app.infra.db import JobRepository, create_db_pool
from app.infra.queue import RedisJobQueue
from app.worker.processor import JobProcessor

logger = logging.getLogger("processing.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


async def run_worker() -> None:
    settings = get_settings()
    pool = await create_db_pool(settings)
    queue = RedisJobQueue.from_settings(settings)

    repository = JobRepository(pool, settings.processing_schema)
    processor = JobProcessor(
        repository=repository,
        settings=settings,
    )

    logger.info("worker started")
    try:
        while True:
            try:
                job_id = await queue.dequeue(settings.queue_pop_timeout_seconds)
                if not job_id:
                    await asyncio.sleep(settings.worker_idle_sleep_seconds)
                    continue

                logger.info("processing job_id=%s", job_id)
                started = time.monotonic()
                await processor.process_job(job_id)
                elapsed_ms = int((time.monotonic() - started) * 1000)
                logger.info("processed job_id=%s elapsed_ms=%s", job_id, elapsed_ms)
            except Exception:
                logger.exception("worker loop error")
                await asyncio.sleep(settings.worker_idle_sleep_seconds)
    finally:
        await queue.close()
        await pool.close()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
