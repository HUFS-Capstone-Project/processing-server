from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from app.api.v1.endpoints.business_hours import router
from app.domain.business_hours import (
    BusinessHoursCreateOutcome,
    BusinessHoursFetchStatus,
    BusinessHoursJobRecord,
    BusinessHoursJobStatus,
    BusinessHoursJobSubmission,
    BusinessHoursPlaceCacheRecord,
)


def _job(job_id=None, *, status=BusinessHoursJobStatus.QUEUED) -> BusinessHoursJobRecord:
    now = datetime.now(timezone.utc)
    return BusinessHoursJobRecord(
        job_id=job_id or uuid4(),
        kakao_place_id="123",
        place_url="https://place.map.kakao.com/123",
        status=status,
        error_code=None,
        error_message=None,
        created_at=now,
        updated_at=now,
    )


def _detail(job_id=None, *, status=BusinessHoursFetchStatus.SUCCEEDED) -> BusinessHoursPlaceCacheRecord:
    now = datetime.now(timezone.utc)
    return BusinessHoursPlaceCacheRecord(
        kakao_place_id="123",
        place_url="https://place.map.kakao.com/123",
        place_name="Test Place",
        business_hours={"daily_hours": []},
        business_hours_raw="raw hours",
        business_hours_status=status,
        business_hours_fetched_at=now,
        business_hours_expires_at=now,
        business_hours_source="kakao_place_crawl",
        business_hours_job_id=job_id,
        last_error="debug error" if status == BusinessHoursFetchStatus.FAILED else None,
        created_at=now,
        updated_at=now,
        version=1,
    )


class FakeBusinessHoursService:
    def __init__(self, outcome: BusinessHoursCreateOutcome) -> None:
        self.outcome = outcome
        self.submission: BusinessHoursJobSubmission | None = None

    async def create_job(self, submission: BusinessHoursJobSubmission) -> BusinessHoursCreateOutcome:
        self.submission = submission
        return self.outcome


class FakeBusinessHoursRepository:
    def __init__(self, job: BusinessHoursJobRecord, detail: BusinessHoursPlaceCacheRecord) -> None:
        self.job = job
        self.detail = detail

    async def get_business_hours_job(self, job_id: UUID):
        return self.job if job_id == self.job.job_id else None

    async def get_business_hours_job_detail(self, job_id: UUID):
        return self.detail if job_id == self.job.job_id else None

    async def get_business_hours_detail(self, kakao_place_id: str):
        return self.detail if kakao_place_id == self.detail.kakao_place_id else None


def _client(service=None, repository=None) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    if service:
        app.state.business_hours_service = service
    if repository:
        app.state.business_hours_repository = repository
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {"X-Internal-Api-Key": "test-internal-key"}


def _request(call):
    try:
        return call()
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def test_post_business_hours_job_returns_snake_case_public_response() -> None:
    job = _job()
    detail = _detail(job.job_id)
    service = FakeBusinessHoursService(
        BusinessHoursCreateOutcome(
            job=job,
            place_cache=detail,
            job_created=True,
            enqueued=True,
            cache_hit=False,
        )
    )

    response = _request(
        lambda: _client(service=service).post(
            "/business-hours/jobs",
            json={
                "kakaoPlaceId": "123",
                "placeUrl": "https://place.map.kakao.com/123",
                "placeName": "Test Place",
            },
            headers=_headers(),
        )
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["cache_hit"] is False
    assert payload["job"] == {"job_id": str(job.job_id), "status": "QUEUED"}
    assert payload["place"]["kakao_place_id"] == "123"
    assert payload["place"]["business_hours_status"] == "SUCCEEDED"
    assert "jobCreated" not in payload
    assert "enqueued" not in payload
    assert "cacheHit" not in payload
    assert "business_hours_raw" not in payload["place"]
    assert service.submission is not None


def test_get_business_hours_job_returns_public_status_response() -> None:
    job = _job()
    detail = _detail(job.job_id)

    response = _request(
        lambda: _client(repository=FakeBusinessHoursRepository(job, detail)).get(
            f"/business-hours/jobs/{job.job_id}",
            headers=_headers(),
        )
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["job"]["status"] == "QUEUED"
    assert payload["place"]["business_hours_status"] == "SUCCEEDED"
    assert "businessHoursStatus" not in payload["place"]


def test_get_business_hours_place_hides_raw_internal_fields() -> None:
    job = _job()
    detail = _detail(job.job_id)

    response = _request(
        lambda: _client(repository=FakeBusinessHoursRepository(job, detail)).get(
            "/business-hours/places/123",
            headers=_headers(),
        )
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["business_hours"] == {"daily_hours": []}
    assert "business_hours_raw" not in payload
    assert "business_hours_source" not in payload
    assert "version" not in payload


def test_get_business_hours_debug_result_returns_internal_fields() -> None:
    job = _job(status=BusinessHoursJobStatus.FAILED)
    detail = _detail(job.job_id, status=BusinessHoursFetchStatus.FAILED)

    response = _request(
        lambda: _client(repository=FakeBusinessHoursRepository(job, detail)).get(
            f"/business-hours/jobs/{job.job_id}/debug-result",
            headers=_headers(),
        )
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["job"]["status"] == "FAILED"
    assert payload["place"]["business_hours_raw"] == "raw hours"
    assert payload["place"]["business_hours_source"] == "kakao_place_crawl"
    assert payload["place"]["last_error"] == "debug error"
    assert payload["place"]["version"] == 1
