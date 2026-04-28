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
        },
        error_message=None,
        updated_at=datetime.now(timezone.utc),
    )

    dumped = response.model_dump()

    assert dumped["extraction_result"]["store_name"] == "커먼맨션"
    assert dumped["extraction_result"]["certainty"] == "high"


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
