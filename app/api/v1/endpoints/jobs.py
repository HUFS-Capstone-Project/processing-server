from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.security import require_internal_api_key
from app.domain.job import CreateJobCommand, InvalidJobRequest, JobService, JobStatus
from app.infra.db.repository import JobRepository
from app.services.crawler.instagram_reel import is_instagram_media_url
from app.schemas.jobs import (
    ApiErrorResponse,
    CreateJobRequest,
    CreateJobResponse,
    JobResultResponse,
    JobStatusResponse,
)

router = APIRouter()


def infer_source(source_url: str) -> str | None:
    if is_instagram_media_url(source_url):
        return "instagram"
    return "web"


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
        source_url=job.source_url,
        source=infer_source(job.source_url),
        status=job.status,
        caption=result.caption if result else None,
        instagram_meta=result.instagram_meta if result else None,
        extraction_result=result.extraction_result if result else None,
        place_candidates=result.place_candidates if result else [],
        selected_places=result.selected_places if result else [],
        error_message=job.error_message,
        updated_at=job.updated_at,
    )
