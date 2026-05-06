from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.core.config import Settings
from app.domain.business_hours import (
    BusinessHoursDetailRecord,
    BusinessHoursDetailStatus,
    BusinessHoursJobRecord,
    BusinessHoursJobStatus,
    BusinessHoursParseResult,
)
from app.services.business_hours import KakaoPlaceCrawlError
from app.worker.business_hours_processor import BusinessHoursWorker


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _job() -> BusinessHoursJobRecord:
    now = datetime.now(timezone.utc)
    return BusinessHoursJobRecord(
        job_id=uuid4(),
        kakao_place_id="123",
        place_url="https://place.map.kakao.com/123",
        status=BusinessHoursJobStatus.PENDING,
        error_code=None,
        error_message=None,
        created_at=now,
        updated_at=now,
    )


def _detail(job: BusinessHoursJobRecord) -> BusinessHoursDetailRecord:
    now = datetime.now(timezone.utc)
    return BusinessHoursDetailRecord(
        kakao_place_id=job.kakao_place_id,
        place_url=job.place_url,
        place_name=None,
        business_hours=None,
        business_hours_raw=None,
        business_hours_status=BusinessHoursDetailStatus.PENDING,
        business_hours_fetched_at=None,
        business_hours_expires_at=None,
        business_hours_source=None,
        business_hours_job_id=job.job_id,
        last_error=None,
        created_at=now,
        updated_at=now,
        version=1,
    )


class FakeRepository:
    def __init__(self, job: BusinessHoursJobRecord) -> None:
        self.job = job
        self.detail = _detail(job)
        self.completed_kwargs = None

    async def claim_business_hours_job(self, job_id):
        if job_id != self.job.job_id:
            return None
        return self.job, self.detail

    async def complete_business_hours_job(self, **kwargs):
        self.completed_kwargs = kwargs
        return self.job, self.detail


def test_business_hours_worker_stores_success(monkeypatch) -> None:
    job = _job()
    repo = FakeRepository(job)

    async def fake_fetch(place_url, settings):
        return BusinessHoursParseResult(
            status=BusinessHoursDetailStatus.SUCCESS,
            business_hours={"time_ranges": [{"open": "10:00", "close": "23:30"}]},
            raw_text="월요일 10:00 ~ 23:30",
        )

    monkeypatch.setattr("app.worker.business_hours_processor.fetch_kakao_place_business_hours", fake_fetch)
    worker = BusinessHoursWorker(repository=repo, settings=Settings())

    outcome = _run(worker.process_job(job.job_id))

    assert outcome.succeeded is True
    assert repo.completed_kwargs["detail_status"] == BusinessHoursDetailStatus.SUCCESS
    assert repo.completed_kwargs["job_status"] == BusinessHoursJobStatus.SUCCEEDED
    assert repo.completed_kwargs["expires_in_seconds"] == Settings().business_hours_success_ttl_seconds


def test_business_hours_worker_stores_not_found_as_succeeded(monkeypatch) -> None:
    job = _job()
    repo = FakeRepository(job)

    async def fake_fetch(place_url, settings):
        return BusinessHoursParseResult(
            status=BusinessHoursDetailStatus.NOT_FOUND,
            business_hours=None,
            raw_text=None,
        )

    monkeypatch.setattr("app.worker.business_hours_processor.fetch_kakao_place_business_hours", fake_fetch)
    worker = BusinessHoursWorker(repository=repo, settings=Settings())

    outcome = _run(worker.process_job(job.job_id))

    assert outcome.succeeded is True
    assert repo.completed_kwargs["detail_status"] == BusinessHoursDetailStatus.NOT_FOUND
    assert repo.completed_kwargs["job_status"] == BusinessHoursJobStatus.SUCCEEDED


def test_business_hours_worker_stores_crawl_failed(monkeypatch) -> None:
    job = _job()
    repo = FakeRepository(job)

    async def fake_fetch(place_url, settings):
        raise KakaoPlaceCrawlError("timeout")

    monkeypatch.setattr("app.worker.business_hours_processor.fetch_kakao_place_business_hours", fake_fetch)
    worker = BusinessHoursWorker(repository=repo, settings=Settings())

    outcome = _run(worker.process_job(job.job_id))

    assert outcome.succeeded is False
    assert repo.completed_kwargs["detail_status"] == BusinessHoursDetailStatus.CRAWL_FAILED
    assert repo.completed_kwargs["job_status"] == BusinessHoursJobStatus.FAILED
    assert repo.completed_kwargs["expires_in_seconds"] == Settings().business_hours_crawl_failed_ttl_seconds
