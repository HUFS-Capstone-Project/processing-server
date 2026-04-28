from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from app.core.config import Settings
from app.domain.job import (
    CrawlArtifact,
    ExtractionCertainty,
    ExtractionResult,
    JobRecord,
    JobStatus,
)
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


class FakeRepository:
    def __init__(self, job: JobRecord) -> None:
        self._job = job
        self.saved_result: dict | None = None
        self.succeeded = False
        self.failed: str | None = None

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

    async def mark_failed(self, job_id: UUID, error_message: str):
        self.failed = error_message
        return self._job


class FakeExtractionClient:
    def __init__(self, result: ExtractionResult | None) -> None:
        self.result = result
        self.calls: list[dict[str, object]] = []

    async def extract(
        self,
        *,
        text: str,
        source_url: str,
        media_type: str | None,
    ) -> ExtractionResult | None:
        self.calls.append(
            {
                "text": text,
                "source_url": source_url,
                "media_type": media_type,
            }
        )
        return self.result


class FailingExtractionClient:
    async def extract(
        self,
        *,
        text: str,
        source_url: str,
        media_type: str | None,
    ) -> ExtractionResult | None:
        raise RuntimeError("endpoint unavailable")


def _new_job() -> JobRecord:
    now = datetime.now(timezone.utc)
    return JobRecord(
        job_id=uuid4(),
        room_id=uuid4(),
        source_url="https://www.instagram.com/reel/example/",
        status=JobStatus.QUEUED,
        error_message=None,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_success(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
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
        settings=settings,
    )

    _run(processor.process_job(job.job_id))

    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["caption"] == "#yeonnamcafe review"
    assert repo.saved_result["extraction_result"] is None
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_passes_caption_to_extraction_client(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()
    extractor = FakeExtractionClient(
        ExtractionResult(
            store_name="Common Mansion",
            address="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            store_name_evidence="Common Mansion",
            address_evidence="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            certainty=ExtractionCertainty.HIGH,
        )
    )

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            media_type="reel",
            caption="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            instagram_meta=None,
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
        extraction_client=extractor,
    )

    _run(processor.process_job(job.job_id))

    assert extractor.calls == [
        {
            "text": "Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            "source_url": job.source_url,
            "media_type": "reel",
        }
    ]
    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["extraction_result"] == {
        "store_name": "Common Mansion",
        "address": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "store_name_evidence": "Common Mansion",
        "address_evidence": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "certainty": "high",
    }
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_succeeds_when_extraction_client_fails(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            media_type="reel",
            caption="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            instagram_meta=None,
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
        extraction_client=FailingExtractionClient(),
    )

    _run(processor.process_job(job.job_id))

    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["caption"] == "Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul"
    assert repo.saved_result["extraction_result"] is None
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_marks_failed_on_error(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        raise RuntimeError("temporary timeout while crawling")

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
    )

    _run(processor.process_job(job.job_id))

    assert repo.failed is not None
