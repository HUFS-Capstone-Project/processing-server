from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.domain.business_hours import (
    BusinessHoursCreateOutcome,
    BusinessHoursPlaceCacheRecord,
    BusinessHoursFetchStatus,
    BusinessHoursEnqueueError,
    BusinessHoursJobRecord,
    BusinessHoursJobStatus,
    BusinessHoursJobSubmission,
    BusinessHoursService,
    InvalidBusinessHoursRequest,
)
from app.infra.db.business_hours_repository import BusinessHoursRepository


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _job(status: BusinessHoursJobStatus = BusinessHoursJobStatus.QUEUED) -> BusinessHoursJobRecord:
    now = datetime.now(timezone.utc)
    return BusinessHoursJobRecord(
        job_id=uuid4(),
        kakao_place_id="123",
        place_url="https://place.map.kakao.com/123",
        status=status,
        error_code=None,
        error_message=None,
        created_at=now,
        updated_at=now,
    )


def _detail(
    status: BusinessHoursFetchStatus = BusinessHoursFetchStatus.PENDING,
    *,
    expires_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> BusinessHoursPlaceCacheRecord:
    now = datetime.now(timezone.utc)
    return BusinessHoursPlaceCacheRecord(
        kakao_place_id="123",
        place_url="https://place.map.kakao.com/123",
        place_name="Test Place",
        business_hours=None,
        business_hours_raw=None,
        business_hours_status=status,
        business_hours_fetched_at=None,
        business_hours_expires_at=expires_at,
        business_hours_source=None,
        business_hours_job_id=uuid4(),
        last_error=None,
        created_at=now,
        updated_at=updated_at or now,
        version=1,
    )


class FakeRepository:
    def __init__(self, outcome: BusinessHoursCreateOutcome) -> None:
        self.outcome = outcome
        self.enqueue_failed_called = False

    async def prepare_business_hours_job(self, **kwargs) -> BusinessHoursCreateOutcome:
        self.prepare_kwargs = kwargs
        return self.outcome

    async def mark_business_hours_enqueue_failed(self, **kwargs) -> BusinessHoursCreateOutcome:
        self.enqueue_failed_called = True
        return BusinessHoursCreateOutcome(
            job=_job(BusinessHoursJobStatus.FAILED),
            place_cache=_detail(BusinessHoursFetchStatus.FAILED),
            job_created=True,
            enqueued=False,
            cache_hit=False,
        )


class FakeQueue:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.enqueued = []

    async def enqueue(self, job_id):
        if self.fail:
            raise RuntimeError("redis down")
        self.enqueued.append(job_id)


def test_create_business_hours_job_enqueues_new_job() -> None:
    job = _job()
    detail = _detail()
    repo = FakeRepository(
        BusinessHoursCreateOutcome(
            job=job,
            place_cache=detail,
            job_created=True,
            enqueued=False,
            cache_hit=False,
        )
    )
    queue = FakeQueue()
    service = BusinessHoursService(repo, queue, stale_timeout_seconds=900, enqueue_failed_ttl_seconds=600)

    outcome = _run(
        service.create_job(
            BusinessHoursJobSubmission(
                kakao_place_id="123",
                place_url="https://place.map.kakao.com/123",
                place_name="Test Place",
            )
        )
    )

    assert outcome.job_created is True
    assert outcome.enqueued is True
    assert queue.enqueued == [job.job_id]


def test_create_business_hours_job_accepts_http_kakao_url_and_normalizes_to_https() -> None:
    job = _job()
    repo = FakeRepository(
        BusinessHoursCreateOutcome(
            job=job,
            place_cache=_detail(),
            job_created=True,
            enqueued=False,
            cache_hit=False,
        )
    )
    service = BusinessHoursService(
        repo,
        FakeQueue(),
        stale_timeout_seconds=900,
        enqueue_failed_ttl_seconds=600,
    )

    _run(
        service.create_job(
            BusinessHoursJobSubmission(
                kakao_place_id="123",
                place_url="http://place.map.kakao.com/123",
            )
        )
    )

    assert repo.prepare_kwargs["place_url"] == "https://place.map.kakao.com/123"


def test_create_business_hours_job_uses_valid_cache_without_enqueue() -> None:
    job = _job(BusinessHoursJobStatus.SUCCEEDED)
    detail = _detail(
        BusinessHoursFetchStatus.SUCCEEDED,
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
    )
    repo = FakeRepository(
        BusinessHoursCreateOutcome(
            job=job,
            place_cache=detail,
            job_created=False,
            enqueued=False,
            cache_hit=True,
        )
    )
    queue = FakeQueue()
    service = BusinessHoursService(repo, queue, stale_timeout_seconds=900, enqueue_failed_ttl_seconds=600)

    outcome = _run(
        service.create_job(
            BusinessHoursJobSubmission(
                kakao_place_id="123",
                place_url="https://place.map.kakao.com/123",
            )
        )
    )

    assert outcome.cache_hit is True
    assert queue.enqueued == []


def test_create_business_hours_job_rejects_non_kakao_url() -> None:
    service = BusinessHoursService(
        FakeRepository(
            BusinessHoursCreateOutcome(
                job=None,
                place_cache=_detail(),
                job_created=False,
                enqueued=False,
                cache_hit=False,
            )
        ),
        FakeQueue(),
        stale_timeout_seconds=900,
        enqueue_failed_ttl_seconds=600,
    )

    with pytest.raises(InvalidBusinessHoursRequest):
        _run(
            service.create_job(
                BusinessHoursJobSubmission(
                    kakao_place_id="123",
                    place_url="https://example.com/123",
                )
            )
        )


def test_create_business_hours_job_marks_enqueue_failed() -> None:
    job = _job()
    repo = FakeRepository(
        BusinessHoursCreateOutcome(
            job=job,
            place_cache=_detail(),
            job_created=True,
            enqueued=False,
            cache_hit=False,
        )
    )
    service = BusinessHoursService(
        repo,
        FakeQueue(fail=True),
        stale_timeout_seconds=900,
        enqueue_failed_ttl_seconds=600,
    )

    with pytest.raises(BusinessHoursEnqueueError):
        _run(
            service.create_job(
                BusinessHoursJobSubmission(
                    kakao_place_id="123",
                    place_url="https://place.map.kakao.com/123",
                )
            )
        )

    assert repo.enqueue_failed_called is True


def test_fetching_stale_timeout_detection() -> None:
    stale_detail = _detail(
        BusinessHoursFetchStatus.FETCHING,
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=901),
    )
    fresh_detail = _detail(
        BusinessHoursFetchStatus.FETCHING,
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    assert BusinessHoursRepository._is_stale_fetching(
        stale_detail,
        datetime.now(timezone.utc),
        900,
    )
    assert not BusinessHoursRepository._is_stale_fetching(
        fresh_detail,
        datetime.now(timezone.utc),
        900,
    )
