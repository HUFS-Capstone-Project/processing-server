from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from app.core.config import Settings
from app.domain.job import CrawlArtifact, JobRecord, JobStatus
from app.worker.processor import JobProcessor

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
    delayed: list[tuple[UUID, int]]

    async def enqueue_delayed(self, job_id: UUID, delay_seconds: int) -> None:
        self.delayed.append((job_id, delay_seconds))


class FakeRepository:
    def __init__(self, job: JobRecord) -> None:
        self._job = job
        self.saved_result: dict | None = None
        self.succeeded = False
        self.failed: tuple[str, str] | None = None
        self.retried: tuple[str, str, int] | None = None

    async def claim_job(self, job_id: UUID) -> JobRecord | None:
        if job_id != self._job.job_id:
            return None
        return self._job

    async def upsert_job_result(self, **kwargs):
        self.saved_result = kwargs
        return None

    async def mark_succeeded(self, job_id: UUID):
        self.succeeded = True
        return self._job

    async def mark_for_retry(self, job_id: UUID, error_code: str, error_message: str, delay_seconds: int):
        self.retried = (error_code, error_message, delay_seconds)
        return self._job

    async def mark_failed(self, job_id: UUID, error_code: str, error_message: str):
        self.failed = (error_code, error_message)
        return self._job


def _new_job(*, attempt: int, max_attempts: int) -> JobRecord:
    now = datetime.now(timezone.utc)
    return JobRecord(
        job_id=uuid4(),
        source_url="https://www.instagram.com/reel/example/",
        source_url_hash="hash",
        status=JobStatus.PROCESSING,
        attempt=attempt,
        max_attempts=max_attempts,
        idempotency_key=None,
        source="web",
        room_id="room-1",
        error_code=None,
        error_message=None,
        queued_at=now,
        processing_started_at=now,
        completed_at=None,
        next_retry_at=None,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_success(monkeypatch) -> None:
    job = _new_job(attempt=1, max_attempts=3)
    repo = FakeRepository(job)
    queue = FakeQueue(delayed=[])
    settings = Settings()

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="#yeonnamcafe review",
            media_type="reel",
            caption="#yeonnamcafe review",
            instagram_meta={"caption": "#yeonnamcafe review"},
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        queue=queue,
        settings=settings,
    )

    _run(processor.process_job(job.job_id))

    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["caption"] == "#yeonnamcafe review"
    assert repo.saved_result["raw_candidates"] == []
    assert repo.saved_result["places"] == []
    assert repo.saved_result["kakao_raw"] == {}
    assert repo.failed is None
    assert repo.retried is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_retries_retryable_error(monkeypatch) -> None:
    job = _new_job(attempt=1, max_attempts=3)
    repo = FakeRepository(job)
    queue = FakeQueue(delayed=[])
    settings = Settings()

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        raise RuntimeError("temporary timeout while crawling")

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        queue=queue,
        settings=settings,
    )

    _run(processor.process_job(job.job_id))

    assert repo.retried is not None
    assert queue.delayed != []
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_marks_failed_when_max_attempt_reached(monkeypatch) -> None:
    job = _new_job(attempt=3, max_attempts=3)
    repo = FakeRepository(job)
    queue = FakeQueue(delayed=[])
    settings = Settings()

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        raise RuntimeError("temporary timeout while crawling")

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        queue=queue,
        settings=settings,
    )

    _run(processor.process_job(job.job_id))

    assert repo.failed is not None
    assert repo.retried is None
    assert queue.delayed == []
