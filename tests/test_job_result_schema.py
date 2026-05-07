from __future__ import annotations

from uuid import uuid4

from app.domain.job import JobStatus
from app.schemas.jobs import JobResultResponse


def test_job_result_response_exposes_only_public_fields() -> None:
    response = JobResultResponse(
        job_id=uuid4(),
        status=JobStatus.SUCCEEDED,
        caption_raw="caption",
        instagram_meta={"like_count": 1, "comment_count": 2},
        resolved_places=[
            {
                "kakao_place_id": "123",
                "place_name": "Example",
                "address": "old address",
                "road_address": "road address",
                "longitude": 127.0,
                "latitude": 37.0,
                "category_name": "Food > Cafe",
                "category_group_code": "CE7",
                "place_url": "https://place.map.kakao.com/123",
                "phone": None,
            }
        ],
        error_code=None,
        error_message=None,
    )

    dumped = response.model_dump()

    assert dumped["caption_raw"] == "caption"
    assert dumped["instagram_meta"] == {"like_count": 1, "comment_count": 2}
    assert dumped["resolved_places"][0]["longitude"] == 127.0
    assert "extraction_result" not in dumped
    assert "place_candidates" not in dumped
    assert "primary_place" not in dumped
    assert "selected_places" not in dumped

