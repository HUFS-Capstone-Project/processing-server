from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints.jobs import router
from app.domain.job import JobRecord, JobResultRecord, JobStatus


class FakeJobService:
    def __init__(self, job: JobRecord) -> None:
        self.job = job

    async def get_job(self, job_id: UUID) -> JobRecord | None:
        if job_id == self.job.job_id:
            return self.job
        return None


class FakeJobRepository:
    def __init__(self, result: JobResultRecord) -> None:
        self.result = result

    async def get_job_result(self, job_id: UUID) -> JobResultRecord | None:
        if job_id == self.result.job_id:
            return self.result
        return None


def test_get_job_result_returns_extraction_result() -> None:
    now = datetime.now(timezone.utc)
    job_id = uuid4()
    extraction_result = {
        "store_name": "Common Mansion",
        "address": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "store_name_evidence": "Common Mansion",
        "address_evidence": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "certainty": "high",
    }
    app = FastAPI()
    app.include_router(router)
    app.state.job_service = FakeJobService(
        JobRecord(
            job_id=job_id,
            room_id=uuid4(),
            source_url="https://www.instagram.com/reel/example/",
            status=JobStatus.SUCCEEDED,
            error_message=None,
            created_at=now,
            updated_at=now,
        )
    )
    app.state.job_repository = FakeJobRepository(
        JobResultRecord(
            job_id=job_id,
            caption="Common Mansion review",
            instagram_meta={"media_type": "reel"},
            extraction_result=extraction_result,
            created_at=now,
            updated_at=now,
        )
    )
    client = TestClient(app)

    response = client.get(
        f"/jobs/{job_id}/result",
        headers={"X-Internal-Api-Key": "test-internal-key"},
    )

    assert response.status_code == 200
    assert response.json()["extraction_result"] == extraction_result
