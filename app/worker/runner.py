from __future__ import annotations

import asyncio
import logging
import math
import statistics
import time

from app.core.config import get_settings
from app.infra.db import JobRepository, create_db_pool
from app.infra.kakao import KakaoLocalClient
from app.infra.llm import HFExtractionClient
from app.infra.queue import RedisJobQueue
from app.services.crawler.playwright_service import prewarm_crawler_runtime, shutdown_crawler_runtime
from app.worker.processor import ExtractionPort, JobProcessor

logger = logging.getLogger("processing.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class WorkerMetricsAggregator:
    def __init__(self, interval_seconds: int) -> None:
        self._interval_seconds = max(30, interval_seconds)
        self._window_started_at = time.monotonic()
        self._success = 0
        self._failed = 0
        self._timed_out = 0
        self._elapsed_ms: list[int] = []

    def record(self, *, succeeded: bool, timed_out: bool, elapsed_ms: int) -> None:
        if succeeded:
            self._success += 1
        else:
            self._failed += 1
        if timed_out:
            self._timed_out += 1
        self._elapsed_ms.append(max(0, elapsed_ms))
        self._flush_if_due()

    def flush(self, *, force: bool = False) -> None:
        self._flush_if_due(force=force)

    def _flush_if_due(self, *, force: bool = False) -> None:
        now = time.monotonic()
        total = self._success + self._failed
        if total == 0:
            if force:
                self._window_started_at = now
            return
        if not force and (now - self._window_started_at) < self._interval_seconds:
            return

        avg_ms = int(sum(self._elapsed_ms) / len(self._elapsed_ms))
        median_ms = int(statistics.median(self._elapsed_ms))
        p95_ms = self._p95(self._elapsed_ms)
        success_rate = self._success / total
        timeout_ratio = self._timed_out / total
        window_seconds = int(now - self._window_started_at)
        logger.info(
            (
                "worker metrics window_seconds=%s total=%s succeeded=%s failed=%s "
                "success_rate=%.4f timeout_ratio=%.4f avg_ms=%s median_ms=%s p95_ms=%s"
            ),
            window_seconds,
            total,
            self._success,
            self._failed,
            success_rate,
            timeout_ratio,
            avg_ms,
            median_ms,
            p95_ms,
        )
        self._window_started_at = now
        self._success = 0
        self._failed = 0
        self._timed_out = 0
        self._elapsed_ms = []

    @staticmethod
    def _p95(values: list[int]) -> int:
        sorted_values = sorted(values)
        if not sorted_values:
            return 0
        idx = max(0, math.ceil(len(sorted_values) * 0.95) - 1)
        return int(sorted_values[idx])


def build_extraction_client(settings) -> ExtractionPort | None:
    if not settings.hf_extraction_endpoint_url or not settings.hf_extraction_api_token:
        logger.info("worker extraction client disabled (HF endpoint URL or token is empty)")
        return None
    return HFExtractionClient(settings)


def build_place_search_client(settings):
    if not settings.kakao_rest_api_key:
        logger.warning("worker kakao client disabled (KAKAO_REST_API_KEY is empty)")
        return None
    return KakaoLocalClient(settings)


async def run_worker() -> None:
    settings = get_settings()
    pool = await create_db_pool(settings)
    queue = RedisJobQueue.from_settings(settings)
    metrics = WorkerMetricsAggregator(settings.worker_metrics_log_interval_seconds)

    repository = JobRepository(pool, settings.processing_schema)
    processor = JobProcessor(
        repository=repository,
        settings=settings,
        extraction_client=build_extraction_client(settings),
        place_search_client=build_place_search_client(settings),
    )

    if settings.worker_prewarm_browser:
        try:
            await asyncio.wait_for(
                prewarm_crawler_runtime(settings),
                timeout=max(1, settings.worker_prewarm_timeout_seconds),
            )
        except Exception:
            logger.warning("worker prewarm failed", exc_info=True)

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
                outcome = await processor.process_job(job_id)
                elapsed_ms = int((time.monotonic() - started) * 1000)
                logger.info(
                    "processed job_id=%s elapsed_ms=%s success=%s timed_out=%s",
                    job_id,
                    elapsed_ms,
                    outcome.succeeded,
                    outcome.timed_out,
                )
                if outcome.processed:
                    metrics.record(
                        succeeded=outcome.succeeded,
                        timed_out=outcome.timed_out,
                        elapsed_ms=outcome.elapsed_ms,
                    )
            except Exception:
                logger.exception("worker loop error")
                await asyncio.sleep(settings.worker_idle_sleep_seconds)
    finally:
        metrics.flush(force=True)
        await shutdown_crawler_runtime()
        await queue.close()
        await pool.close()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
