from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints.jobs import router
from app.domain.job import (
    CrawledContentRecord,
    JobRecord,
    JobResultRecord,
    JobStatus,
    LinkStatsRecord,
)


class FakeJobService:
    def __init__(self, job: JobRecord) -> None:
        self.job = job

    async def get_job(self, job_id: UUID) -> JobRecord | None:
        return self.job if job_id == self.job.job_id else None


class FakeJobRepository:
    def __init__(
        self,
        result: JobResultRecord,
        content: CrawledContentRecord | None = None,
        link_stats: LinkStatsRecord | None = None,
    ) -> None:
        self.result = result
        self.content = content
        self.link_stats = link_stats

    async def get_job_result(self, job_id: UUID) -> JobResultRecord | None:
        return self.result if job_id == self.result.job_id else None

    async def get_crawled_content(self, job_id: UUID) -> CrawledContentRecord | None:
        return self.content if self.content and job_id == self.content.job_id else None

    async def get_link_stats(self, job_id: UUID) -> LinkStatsRecord | None:
        return self.link_stats if self.link_stats and job_id == self.link_stats.job_id else None


def _client(
    job: JobRecord,
    result: JobResultRecord,
    content: CrawledContentRecord | None = None,
    link_stats: LinkStatsRecord | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.job_service = FakeJobService(job)
    app.state.job_repository = FakeJobRepository(result, content, link_stats)
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {"X-Internal-Api-Key": "test-internal-key"}


def test_get_job_result_returns_public_contract_only() -> None:
    now = datetime.now(timezone.utc)
    job_id = uuid4()
    place_result = {
        "kakao_place_id": "123",
        "place_name": "Common Mansion",
        "category_name": "Food > Cafe",
        "category_group_code": "CE7",
        "category_group_name": "Cafe",
        "phone": "02-0000-0000",
        "address_name": "Seoul Jongno-gu Sinmunro 2-ga 1-102",
        "road_address_name": "Seoul Jongno-gu Saemunan-ro 1",
        "x": "126.970000",
        "y": "37.570000",
        "place_url": "https://place.map.kakao.com/123",
        "confidence": 0.95,
        "query": "Common Mansion",
        "evidence_text": "Common Mansion 1-102 Sinmunro 2-ga",
        "original_text": "Common Mansion",
    }
    extraction_result = {
        "store_name": "Common Mansion",
        "address": "1-102 Sinmunro 2-ga",
        "certainty": "high",
        "places": [{"store_name": "Common Mansion", "address": "1-102 Sinmunro 2-ga", "certainty": "high"}],
    }
    job = JobRecord(
        job_id=job_id,
        room_id=uuid4(),
        source_url="https://www.instagram.com/reel/example/",
        status=JobStatus.SUCCEEDED,
        error_message=None,
        created_at=now,
        updated_at=now,
    )
    result = JobResultRecord(
        job_id=job_id,
        extraction_result=extraction_result,
        place_candidates=[place_result],
        resolved_places=[place_result],
        created_at=now,
        updated_at=now,
    )
    content = CrawledContentRecord(
        job_id=job_id,
        source_url=job.source_url,
        source_type="INSTAGRAM",
        content_text="Common Mansion review",
        extraction_method="INSTAGRAM_OG_META",
        raw_metadata={"caption": "Common Mansion review"},
        created_at=now,
        updated_at=now,
    )
    link_stats = LinkStatsRecord(
        job_id=job_id,
        source_url=job.source_url,
        source_type="INSTAGRAM",
        like_count=123,
        comment_count=45,
        posted_at="April 2, 2026",
        collected_at=now,
        stats_source="INSTAGRAM_META",
        confidence="MEDIUM",
        unavailable_reason=None,
        raw_stats={"likes": 123, "comments": 45, "posted_at": "April 2, 2026"},
        created_at=now,
        updated_at=now,
    )

    response = _client(job, result, content, link_stats).get(f"/jobs/{job_id}/result", headers=_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "job_id": str(job_id),
        "status": "SUCCEEDED",
        "source_url": "https://www.instagram.com/reel/example/",
        "content": {
            "source_type": "INSTAGRAM",
            "content_text": "Common Mansion review",
            "extraction_method": "INSTAGRAM_OG_META",
        },
        "link_stats": {
            "like_count": 123,
            "comment_count": 45,
            "posted_at": "April 2, 2026",
        },
        "resolved_places": [
            {
                "kakao_place_id": "123",
                "place_name": "Common Mansion",
                "address": "Seoul Jongno-gu Sinmunro 2-ga 1-102",
                "road_address": "Seoul Jongno-gu Saemunan-ro 1",
                "longitude": 126.97,
                "latitude": 37.57,
                "category_name": "Food > Cafe",
                "category_group_code": "CE7",
                "place_url": "https://place.map.kakao.com/123",
                "phone": "02-0000-0000",
            }
        ],
        "error_code": None,
        "error_message": None,
    }
    assert "extraction_result" not in payload
    assert "place_candidates" not in payload
    assert "instagram_meta" not in payload
    assert "caption_raw" not in payload
    assert "primary_place" not in payload
    assert "selected_places" not in payload
    assert "query" not in payload["resolved_places"][0]
    assert "category_group_name" not in payload["resolved_places"][0]
    assert "x" not in payload["resolved_places"][0]
    assert "y" not in payload["resolved_places"][0]


def test_get_job_debug_result_returns_internal_fields() -> None:
    now = datetime.now(timezone.utc)
    job_id = uuid4()
    job = JobRecord(
        job_id=job_id,
        room_id=uuid4(),
        source_url="https://example.com/post",
        status=JobStatus.FAILED,
        error_message="failed",
        created_at=now,
        updated_at=now,
        attempt_count=2,
        max_attempts=3,
        error_code="RETRYABLE_TIMEOUT",
    )
    result = JobResultRecord(
        job_id=job_id,
        extraction_result={"raw": "extraction"},
        place_candidates=[{"query": "candidate", "confidence": 0.5}],
        resolved_places=[],
        created_at=now,
        updated_at=now,
    )
    content = CrawledContentRecord(
        job_id=job_id,
        source_url=job.source_url,
        source_type="GENERIC_WEB",
        content_text="debug body",
        extraction_method="GENERIC_WEB_INNER_TEXT",
        raw_metadata={"raw": "meta"},
        created_at=now,
        updated_at=now,
    )

    response = _client(job, result, content).get(f"/jobs/{job_id}/debug-result", headers=_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["attempt_count"] == 2
    assert payload["error_code"] == "RETRYABLE_TIMEOUT"
    assert payload["content"]["raw_metadata"] == {"raw": "meta"}
    assert payload["extraction_result"] == {"raw": "extraction"}
    assert payload["place_candidates"] == [{"query": "candidate", "confidence": 0.5}]

