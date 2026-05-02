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
        "places": [
            {
                "store_name": "Common Mansion",
                "address": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
                "store_name_evidence": "Common Mansion",
                "address_evidence": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
                "certainty": "high",
            }
        ],
    }
    selected_place = {
        "kakao_place_id": "123",
        "place_name": "Common Mansion",
        "category_name": "음식점 > 카페",
        "category_group_code": "CE7",
        "category_group_name": "카페",
        "phone": None,
        "address_name": "서울 종로구 신문로2가 1-102",
        "road_address_name": "서울 종로구 새문안로 1",
        "x": "126.970000",
        "y": "37.570000",
        "place_url": "https://place.map.kakao.com/123",
        "confidence": 0.95,
        "source_keyword": "Common Mansion",
        "source_sentence": "Common Mansion 1-102 Sinmunro 2-ga",
        "raw_candidate": "Common Mansion",
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
            place_candidates=[selected_place],
            selected_place=selected_place,
            selected_places=[selected_place],
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
    assert response.json()["place_candidates"] == [selected_place]
    assert response.json()["selected_place"] == selected_place
    assert response.json()["selected_places"] == [selected_place]
