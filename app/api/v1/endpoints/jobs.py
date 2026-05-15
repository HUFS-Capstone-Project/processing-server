from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.security import require_internal_api_key
from app.domain.job import CreateJobCommand, InvalidJobRequest, JobService, JobStatus
from app.domain.source_classifier import classify_source_url
from app.infra.db.repository import JobRepository
from app.schemas.jobs import (
    ApiErrorResponse,
    CreateJobRequest,
    CreateJobResponse,
    InstagramMetaResponse,
    JobDebugResultResponse,
    JobResultResponse,
    JobStatusResponse,
    ResolvedPlaceResponse,
)

router = APIRouter()


def infer_source(source_url: str) -> str | None:
    return classify_source_url(source_url)


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
    description="Validate URL, create a processing job record, and enqueue it.",
    responses={401: {"model": ApiErrorResponse}, 422: {"model": ApiErrorResponse}},
)
async def create_job(
    payload: CreateJobRequest,
    service: JobService = Depends(get_job_service),
) -> CreateJobResponse:
    try:
        job = await service.create_job(
            CreateJobCommand(
                url=str(payload.url),
                room_id=payload.room_id,
            )
        )
    except InvalidJobRequest as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "INVALID_URL", "message": str(exc)},
        ) from exc

    return CreateJobResponse(
        job_id=job.job_id,
        status=job.status,
        source_url=job.source_url,
        source=infer_source(job.source_url),
        created_at=job.created_at,
    )


@router.get(
    "/jobs/{jobId}",
    response_model=JobStatusResponse,
    dependencies=[Depends(require_internal_api_key)],
    summary="Get job status",
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
        source_url=job.source_url,
        source=infer_source(job.source_url),
        status=job.status,
        error_message=job.error_message,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


@router.get(
    "/jobs/{jobId}/result",
    response_model=JobResultResponse,
    dependencies=[Depends(require_internal_api_key)],
    summary="Get job result",
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

    if job.status in {JobStatus.QUEUED, JobStatus.PROCESSING}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "RESULT_NOT_READY", "message": f"Job is currently {job.status.value}."},
        )

    result = await repository.get_job_result(jobId)

    return JobResultResponse(
        job_id=job.job_id,
        status=job.status,
        caption_raw=result.caption if result else None,
        instagram_meta=_instagram_meta_response(result.instagram_meta if result else None),
        resolved_places=[
            _resolved_place_response(place)
            for place in (result.resolved_places if result else [])
        ],
        error_code=job.error_code,
        error_message=job.error_message,
    )


@router.get(
    "/jobs/{jobId}/debug-result",
    response_model=JobDebugResultResponse,
    dependencies=[Depends(require_internal_api_key)],
    summary="Get internal job debug result",
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
    return JobDebugResultResponse(
        job_id=job.job_id,
        room_id=job.room_id,
        source_url=job.source_url,
        normalized_source_url=job.normalized_source_url,
        source=infer_source(job.source_url),
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
        caption_raw=result.caption if result else None,
        instagram_meta=result.instagram_meta if result else None,
        extraction_result=result.extraction_result if result else None,
        place_candidates=result.place_candidates if result else [],
        resolved_places=result.resolved_places if result else [],
    )


def _instagram_meta_response(raw: dict[str, object] | None) -> InstagramMetaResponse | None:
    if raw is None:
        return None
    return InstagramMetaResponse(
        like_count=_optional_int(raw.get("like_count", raw.get("likes"))),
        comment_count=_optional_int(raw.get("comment_count", raw.get("comments"))),
    )


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


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
