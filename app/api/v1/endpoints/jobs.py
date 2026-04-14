from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.core.security import require_internal_api_key
from app.domain.job import CreateJobCommand, InvalidJobRequest, JobService, JobStatus
from app.infra.db.repository import JobRepository
from app.schemas.jobs import (
    ApiErrorResponse,
    CreateJobRequest,
    CreateJobResponse,
    JobResultResponse,
    JobStatusResponse,
    PlaceResponse,
)

router = APIRouter()


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
    response: Response,
    service: JobService = Depends(get_job_service),
) -> CreateJobResponse:
    try:
        job, created = await service.create_job(
            CreateJobCommand(
                url=str(payload.url),
                idempotency_key=payload.idempotency_key,
                source=payload.source,
                room_id=payload.room_id,
            )
        )
    except InvalidJobRequest as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "INVALID_URL", "message": str(exc)},
        ) from exc

    if not created:
        response.status_code = status.HTTP_200_OK

    return CreateJobResponse(
        job_id=job.job_id,
        status=job.status,
        created_at=job.created_at,
        idempotent_reused=not created,
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
        status=job.status,
        attempt=job.attempt,
        max_attempts=job.max_attempts,
        error_code=job.error_code,
        error_message=job.error_message,
        created_at=job.created_at,
        queued_at=job.queued_at,
        processing_started_at=job.processing_started_at,
        completed_at=job.completed_at,
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

    if job.status == JobStatus.FAILED:
        return JobResultResponse(
            job_id=job.job_id,
            status=job.status,
            caption=result.caption if result else None,
            media_type=result.media_type if result else None,
            instagram_meta=result.instagram_meta if result else None,
            raw_candidates=result.raw_candidates if result else [],
            places=[],
            error_code=job.error_code,
            error_message=job.error_message,
            completed_at=job.completed_at,
        )

    places = [PlaceResponse(**place) for place in (result.places if result else [])]
    return JobResultResponse(
        job_id=job.job_id,
        status=job.status,
        caption=result.caption if result else None,
        media_type=result.media_type if result else None,
        instagram_meta=result.instagram_meta if result else None,
        raw_candidates=result.raw_candidates if result else [],
        places=places,
        error_code=job.error_code,
        error_message=job.error_message,
        completed_at=job.completed_at,
    )
