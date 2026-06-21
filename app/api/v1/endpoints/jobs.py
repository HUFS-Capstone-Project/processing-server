from __future__ import annotations

from datetime import datetime, timezone
import re
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.security import require_internal_api_key
from app.domain.job import (
    CreateJobCommand,
    InstagramRateLimited,
    InvalidJobRequest,
    JobService,
    JobStatus,
)
from app.domain.job.service import INSTAGRAM_RATE_LIMITED_ERROR_CODE
from app.domain.url_contract import crawl_url_for
from app.infra.db.repository import JobRepository
from app.schemas.errors import ApiErrorResponse, InstagramRateLimitErrorResponse
from app.schemas.jobs import (
    CrawledContentResponse,
    CreateJobRequest,
    CreateJobResponse,
    JobDebugResultResponse,
    JobResultResponse,
    LinkStatsResponse,
    JobStatusResponse,
    ResolvedPlaceResponse,
)

router = APIRouter()

_COOLDOWN_SECONDS_PATTERN = re.compile(r"(?:Retry after|for)\s+(\d+)\s+seconds", re.IGNORECASE)

_UNAUTHORIZED_RESPONSE = {
    status.HTTP_401_UNAUTHORIZED: {
        "model": ApiErrorResponse,
        "description": "Missing or invalid X-Internal-Api-Key header.",
    },
}
_NOT_FOUND_RESPONSE = {
    status.HTTP_404_NOT_FOUND: {
        "model": ApiErrorResponse,
        "description": "Job not found.",
    },
}


def get_job_service(request: Request) -> JobService:
    return request.app.state.job_service


def get_job_repository(request: Request) -> JobRepository:
    return request.app.state.job_repository


@router.post(
    "/jobs",
    response_model=CreateJobResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_internal_api_key)],
    summary="Create processing job",
    description=(
        "Validate URL, create a processing job record, and enqueue it. "
        "Instagram URLs are rejected with 429 while the global Instagram cooldown is active."
    ),
    responses={
        **_UNAUTHORIZED_RESPONSE,
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "model": ApiErrorResponse,
            "description": "Request validation failed or the submitted URL is invalid.",
        },
        status.HTTP_429_TOO_MANY_REQUESTS: {
            "model": InstagramRateLimitErrorResponse,
            "description": (
                "Instagram global cooldown is active. The job is not created and "
                "cooldown_seconds indicates when the client may retry."
            ),
        },
    },
)
async def create_job(
    payload: CreateJobRequest,
    service: JobService = Depends(get_job_service),
) -> CreateJobResponse:
    try:
        job = await service.create_job(
            CreateJobCommand(
                original_url=payload.original_url,
                room_id=payload.room_id,
            )
        )
    except InvalidJobRequest as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "INVALID_URL", "message": str(exc)},
        ) from exc
    except InstagramRateLimited as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "code": INSTAGRAM_RATE_LIMITED_ERROR_CODE,
                "message": str(exc),
                "retryable": True,
                "cooldown_seconds": exc.cooldown_seconds,
            },
        ) from exc

    return CreateJobResponse(
        job_id=job.job_id,
        status=job.status,
        original_url=job.original_url,
        canonical_url=job.canonical_url,
        created_at=job.created_at,
    )


@router.get(
    "/jobs/{jobId}",
    response_model=JobStatusResponse,
    dependencies=[Depends(require_internal_api_key)],
    summary="Get job status",
    responses={
        **_UNAUTHORIZED_RESPONSE,
        **_NOT_FOUND_RESPONSE,
    },
)
async def get_job_status(
    jobId: UUID,
    service: JobService = Depends(get_job_service),
) -> JobStatusResponse:
    job = await service.get_job(jobId)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "JOB_NOT_FOUND", "message": "Job not found."},
        )

    return JobStatusResponse(
        job_id=job.job_id,
        room_id=job.room_id,
        original_url=job.original_url,
        canonical_url=job.canonical_url,
        status=job.status,
        error_code=job.error_code,
        error_message=job.error_message,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


@router.get(
    "/jobs/{jobId}/result",
    response_model=JobResultResponse,
    dependencies=[Depends(require_internal_api_key)],
    summary="Get job result",
    responses={
        **_UNAUTHORIZED_RESPONSE,
        **_NOT_FOUND_RESPONSE,
        status.HTTP_409_CONFLICT: {
            "model": ApiErrorResponse,
            "description": "Job is still QUEUED or PROCESSING; result is not ready yet.",
        },
    },
)
async def get_job_result(
    jobId: UUID,
    service: JobService = Depends(get_job_service),
    repository: JobRepository = Depends(get_job_repository),
) -> JobResultResponse:
    job = await service.get_job(jobId)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "JOB_NOT_FOUND", "message": "Job not found."},
        )

    result = await repository.get_job_result(jobId)
    if job.status in {JobStatus.QUEUED, JobStatus.PROCESSING} and not result:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "RESULT_NOT_READY", "message": f"Job is currently {job.status.value}."},
        )

    content = await repository.get_crawled_content(jobId)
    link_stats = await repository.get_link_stats(jobId)

    return JobResultResponse(
        job_id=job.job_id,
        status=job.status,
        original_url=job.original_url,
        canonical_url=job.canonical_url,
        crawl_url=_crawl_url(job, content),
        content=_content_response(content),
        link_stats=_link_stats_response(link_stats),
        resolved_places=[
            _resolved_place_response(place)
            for place in (result.resolved_places if result else [])
        ],
        error_code=job.error_code,
        error_message=job.error_message,
        retryable=_is_retryable_failure(job),
        cooldown_seconds=_cooldown_seconds(job, content),
    )


@router.get(
    "/jobs/{jobId}/debug-result",
    response_model=JobDebugResultResponse,
    dependencies=[Depends(require_internal_api_key)],
    summary="Get internal job debug result",
    responses={
        **_UNAUTHORIZED_RESPONSE,
        **_NOT_FOUND_RESPONSE,
    },
)
async def get_job_debug_result(
    jobId: UUID,
    service: JobService = Depends(get_job_service),
    repository: JobRepository = Depends(get_job_repository),
) -> JobDebugResultResponse:
    job = await service.get_job(jobId)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "JOB_NOT_FOUND", "message": "Job not found."},
        )
    result = await repository.get_job_result(jobId)
    content = await repository.get_crawled_content(jobId)
    link_stats = await repository.get_link_stats(jobId)
    return JobDebugResultResponse(
        job_id=job.job_id,
        room_id=job.room_id,
        original_url=job.original_url,
        canonical_url=job.canonical_url,
        crawl_url=_crawl_url(job, content),
        status=job.status,
        attempt_count=job.attempt_count,
        max_attempts=job.max_attempts,
        error_code=job.error_code,
        error_message=job.error_message,
        next_retry_at=job.next_retry_at,
        processing_started_at=job.processing_started_at,
        last_heartbeat_at=job.last_heartbeat_at,
        failed_at=job.failed_at,
        completed_at=job.completed_at,
        created_at=job.created_at,
        updated_at=job.updated_at,
        content=_content_debug_dict(content),
        link_stats=_link_stats_debug_dict(link_stats),
        extraction_result=result.extraction_result if result else None,
        place_candidates=result.place_candidates if result else [],
        resolved_places=result.resolved_places if result else [],
    )


def _content_response(content) -> CrawledContentResponse | None:
    if content is None:
        return None
    return CrawledContentResponse(
        source_type=content.source_type,
        content_text=content.content_text,
        extraction_method=content.extraction_method,
    )


def _crawl_url(job, content) -> str:
    if content is not None:
        return content.crawl_url
    return crawl_url_for(job.original_url)


def _link_stats_response(link_stats) -> LinkStatsResponse | None:
    if link_stats is None:
        return None
    return LinkStatsResponse(
        like_count=link_stats.like_count,
        comment_count=link_stats.comment_count,
        posted_at=link_stats.posted_at,
    )


def _is_retryable_failure(job) -> bool:
    return job.status == JobStatus.FAILED and job.error_code == INSTAGRAM_RATE_LIMITED_ERROR_CODE


def _cooldown_seconds(job, content) -> int | None:
    if not _is_retryable_failure(job):
        return None

    duration = _cooldown_duration_seconds(content)
    if duration is None:
        duration = _cooldown_duration_from_message(job.error_message)
    if duration is None:
        return None

    anchor = job.failed_at or job.updated_at
    if anchor is None:
        return max(0, duration)
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=timezone.utc)
    elapsed_seconds = int((datetime.now(timezone.utc) - anchor).total_seconds())
    return max(0, duration - max(0, elapsed_seconds))


def _cooldown_duration_seconds(content) -> int | None:
    raw_metadata = getattr(content, "raw_metadata", None)
    if not isinstance(raw_metadata, dict):
        return None

    direct = _positive_int(raw_metadata.get("cooldown_seconds"))
    if direct is not None:
        return direct

    instagram_metadata = raw_metadata.get("instagram")
    if isinstance(instagram_metadata, dict):
        return _positive_int(instagram_metadata.get("cooldown_seconds"))
    return None


def _cooldown_duration_from_message(message: str | None) -> int | None:
    if not message:
        return None
    match = _COOLDOWN_SECONDS_PATTERN.search(message)
    if not match:
        return None
    return _positive_int(match.group(1))


def _positive_int(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _content_debug_dict(content) -> dict[str, object] | None:
    if content is None:
        return None
    return {
        "crawl_url": content.crawl_url,
        "source_type": content.source_type,
        "content_text": content.content_text,
        "extraction_method": content.extraction_method,
        "raw_metadata": content.raw_metadata,
    }


def _link_stats_debug_dict(link_stats) -> dict[str, object] | None:
    if link_stats is None:
        return None
    return {
        "crawl_url": link_stats.crawl_url,
        "source_type": link_stats.source_type,
        "like_count": link_stats.like_count,
        "comment_count": link_stats.comment_count,
        "posted_at": link_stats.posted_at,
        "collected_at": link_stats.collected_at,
        "stats_source": link_stats.stats_source,
        "confidence": link_stats.confidence,
        "unavailable_reason": link_stats.unavailable_reason,
        "raw_stats": link_stats.raw_stats,
    }


def _resolved_place_response(place: dict[str, object]) -> ResolvedPlaceResponse:
    return ResolvedPlaceResponse(
        kakao_place_id=str(place.get("kakao_place_id") or ""),
        place_name=str(place.get("place_name") or ""),
        address=_optional_str(place.get("address") or place.get("address_name")),
        road_address=_optional_str(place.get("road_address") or place.get("road_address_name")),
        longitude=_optional_float(place.get("longitude") or place.get("x")),
        latitude=_optional_float(place.get("latitude") or place.get("y")),
        category_name=_optional_str(place.get("category_name")),
        category_group_code=_optional_str(place.get("category_group_code")),
        place_url=_optional_str(place.get("place_url")),
        phone=_optional_str(place.get("phone")),
    )


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
