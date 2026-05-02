from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from app.domain.job import JobStatus
from app.schemas.jobs import JobResultResponse


def test_job_result_response_accepts_extraction_result() -> None:
    response = JobResultResponse(
        job_id=uuid4(),
        source_url="https://www.instagram.com/reel/example/",
        source="instagram",
        status=JobStatus.SUCCEEDED,
        caption="• 커먼맨션\n서울 종로구 신문로2가 1-102",
        instagram_meta=None,
        extraction_result={
            "store_name": "커먼맨션",
            "address": "서울 종로구 신문로2가 1-102",
            "store_name_evidence": "• 커먼맨션",
            "address_evidence": "서울 종로구 신문로2가 1-102",
            "certainty": "high",
            "places": [
                {
                    "store_name": "커먼맨션",
                    "address": "서울 종로구 신문로2가 1-102",
                    "store_name_evidence": "• 커먼맨션",
                    "address_evidence": "서울 종로구 신문로2가 1-102",
                    "certainty": "high",
                }
            ],
        },
        error_message=None,
        updated_at=datetime.now(timezone.utc),
    )

    dumped = response.model_dump()

    assert dumped["extraction_result"]["store_name"] == "커먼맨션"
    assert dumped["extraction_result"]["certainty"] == "high"
    assert dumped["extraction_result"]["places"][0]["store_name"] == "커먼맨션"


def test_job_result_response_allows_missing_extraction_result() -> None:
    response = JobResultResponse(
        job_id=uuid4(),
        source_url="https://example.com/post",
        source="web",
        status=JobStatus.SUCCEEDED,
        caption="caption only",
        instagram_meta=None,
        error_message=None,
        updated_at=datetime.now(timezone.utc),
    )

    assert response.extraction_result is None
    assert response.place_candidates == []
    assert response.selected_places == []


def test_job_result_response_accepts_kakao_place_result() -> None:
    place_result = {
        "kakao_place_id": "123",
        "place_name": "커먼맨션",
        "category_name": "음식점 > 카페",
        "category_group_code": "CE7",
        "category_group_name": "카페",
        "address_name": "서울 종로구 신문로2가 1-102",
        "road_address_name": "서울 종로구 새문안로 1",
        "x": "126.970000",
        "y": "37.570000",
        "place_url": "https://place.map.kakao.com/123",
        "phone": None,
        "confidence": 0.95,
        "source_keyword": "커먼맨션",
        "source_sentence": "브런치 맛집 커먼맨션 입니다",
        "raw_candidate": "커먼맨션",
    }

    response = JobResultResponse(
        job_id=uuid4(),
        source_url="https://www.instagram.com/reel/example/",
        source="instagram",
        status=JobStatus.SUCCEEDED,
        caption="caption",
        instagram_meta=None,
        extraction_result=None,
        place_candidates=[place_result],
        selected_places=[place_result],
        error_message=None,
        updated_at=datetime.now(timezone.utc),
    )

    dumped = response.model_dump()

    assert "selected_place" not in dumped
    assert dumped["selected_places"][0]["place_name"] == "커먼맨션"
    assert dumped["selected_places"][0]["category_group_code"] == "CE7"
    assert dumped["place_candidates"][0]["road_address_name"] == "서울 종로구 새문안로 1"
