from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from app.api.v1.endpoints.business_hours import router
from app.domain.business_hours import (
    BusinessHoursCreateOutcome,
    BusinessHoursDetailRecord,
    BusinessHoursDetailStatus,
    BusinessHoursJobRecord,
    BusinessHoursJobStatus,
    BusinessHoursJobSubmission,
)


def _job(job_id=None) -> BusinessHoursJobRecord:
    now = datetime.now(timezone.utc)
    return BusinessHoursJobRecord(
        job_id=job_id or uuid4(),
        kakao_place_id="123",
        place_url="https://place.map.kakao.com/123",
        status=BusinessHoursJobStatus.PENDING,
        error_code=None,
        error_message=None,
        created_at=now,
        updated_at=now,
    )


def _detail(job_id=None) -> BusinessHoursDetailRecord:
    now = datetime.now(timezone.utc)
    return BusinessHoursDetailRecord(
        kakao_place_id="123",
        place_url="https://place.map.kakao.com/123",
        place_name="Test Place",
        business_hours={"time_ranges": []},
        business_hours_raw="영업 중",
        business_hours_status=BusinessHoursDetailStatus.SUCCESS,
        business_hours_fetched_at=now,
        business_hours_expires_at=now,
        business_hours_source="kakao_place_crawl",
        business_hours_job_id=job_id,
        last_error=None,
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
    def __init__(self, job: BusinessHoursJobRecord, detail: BusinessHoursDetailRecord) -> None:
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


def _request(call):
    try:
        return call()
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def test_post_business_hours_job_returns_camel_case_response() -> None:
    job = _job()
    detail = _detail(job.job_id)
    service = FakeBusinessHoursService(
        BusinessHoursCreateOutcome(
            job=job,
            detail=detail,
            created=True,
            enqueued=True,
            cache_hit=False,
        )
    )
    client = _client(service=service)

    response = _request(
        lambda: client.post(
            "/business-hours/jobs",
            json={
                "kakaoPlaceId": "123",
                "placeUrl": "https://place.map.kakao.com/123",
                "placeName": "Test Place",
            },
            headers={"X-Internal-Api-Key": "test-internal-key"},
        )
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["job"]["jobId"] == str(job.job_id)
    assert payload["place"]["kakaoPlaceId"] == "123"
    assert payload["cacheHit"] is False
    assert service.submission is not None
    assert service.submission.kakao_place_id == "123"


def test_get_business_hours_job_returns_job_and_detail_status() -> None:
    job = _job()
    detail = _detail(job.job_id)
    client = _client(repository=FakeBusinessHoursRepository(job, detail))

    response = _request(
        lambda: client.get(
            f"/business-hours/jobs/{job.job_id}",
            headers={"X-Internal-Api-Key": "test-internal-key"},
        )
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["job"]["status"] == "PENDING"
    assert payload["place"]["businessHoursStatus"] == "SUCCESS"


def test_get_business_hours_place_returns_cached_detail() -> None:
    job = _job()
    detail = _detail(job.job_id)
    client = _client(repository=FakeBusinessHoursRepository(job, detail))

    response = _request(
        lambda: client.get(
            "/business-hours/places/123",
            headers={"X-Internal-Api-Key": "test-internal-key"},
        )
    )

    assert response.status_code == 200
    assert response.json()["businessHoursRaw"] == "영업 중"
