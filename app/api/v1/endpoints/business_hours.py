from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.core.security import require_internal_api_key
from app.domain.business_hours import (
    BusinessHoursCreateOutcome,
    BusinessHoursDetailRecord,
    BusinessHoursEnqueueError,
    BusinessHoursJobRecord,
    BusinessHoursJobSubmission,
    BusinessHoursService,
    InvalidBusinessHoursRequest,
)
from app.infra.db import BusinessHoursRepository
from app.schemas.business_hours import (
    BusinessHoursJobResponse,
    BusinessHoursJobStatusResponse,
    BusinessHoursPlaceResponse,
    CreateBusinessHoursJobRequest,
    CreateBusinessHoursJobResponse,
)

router = APIRouter(prefix="/business-hours")


def get_business_hours_service(request: Request) -> BusinessHoursService:
    return request.app.state.business_hours_service


def get_business_hours_repository(request: Request) -> BusinessHoursRepository:
    return request.app.state.business_hours_repository


@router.post(
    "/jobs",
    response_model=CreateBusinessHoursJobResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_internal_api_key)],
    summary="Create business hours crawling job",
)
async def create_business_hours_job(
    payload: CreateBusinessHoursJobRequest,
    response: Response,
    service: BusinessHoursService = Depends(get_business_hours_service),
) -> CreateBusinessHoursJobResponse:
    try:
        outcome = await service.create_job(
            BusinessHoursJobSubmission(
                kakao_place_id=payload.kakao_place_id,
                place_url=str(payload.place_url),
                place_name=payload.place_name,
            )
        )
    except InvalidBusinessHoursRequest as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "INVALID_PLACE_URL", "message": str(exc)},
        ) from exc
    except BusinessHoursEnqueueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "ENQUEUE_FAILED", "message": str(exc)},
        ) from exc

    if not outcome.created:
        response.status_code = status.HTTP_200_OK
    return _create_response(outcome)


@router.get(
    "/jobs/{jobId}",
    response_model=BusinessHoursJobStatusResponse,
    dependencies=[Depends(require_internal_api_key)],
    summary="Get business hours job status",
)
async def get_business_hours_job(
    jobId: UUID,
    repository: BusinessHoursRepository = Depends(get_business_hours_repository),
) -> BusinessHoursJobStatusResponse:
    job = await repository.get_business_hours_job(jobId)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "BUSINESS_HOURS_JOB_NOT_FOUND", "message": "Business hours job not found."},
        )
    detail = await repository.get_business_hours_job_detail(jobId)
    return BusinessHoursJobStatusResponse(
        job=_job_response(job),
        place=_place_response(detail) if detail else None,
    )


@router.get(
    "/places/{kakaoPlaceId}",
    response_model=BusinessHoursPlaceResponse,
    dependencies=[Depends(require_internal_api_key)],
    summary="Get cached business hours by Kakao place id",
)
async def get_business_hours_place(
    kakaoPlaceId: str,
    repository: BusinessHoursRepository = Depends(get_business_hours_repository),
) -> BusinessHoursPlaceResponse:
    detail = await repository.get_business_hours_detail(kakaoPlaceId)
    if not detail:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "BUSINESS_HOURS_PLACE_NOT_FOUND", "message": "Business hours detail not found."},
        )
    return _place_response(detail)


def _create_response(outcome: BusinessHoursCreateOutcome) -> CreateBusinessHoursJobResponse:
    return CreateBusinessHoursJobResponse(
        job=_job_response(outcome.job) if outcome.job else None,
        place=_place_response(outcome.detail),
        created=outcome.created,
        enqueued=outcome.enqueued,
        cache_hit=outcome.cache_hit,
    )


def _job_response(job: BusinessHoursJobRecord) -> BusinessHoursJobResponse:
    return BusinessHoursJobResponse(
        job_id=job.job_id,
        kakao_place_id=job.kakao_place_id,
        place_url=job.place_url,
        status=job.status,
        error_code=job.error_code,
        error_message=job.error_message,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _place_response(detail: BusinessHoursDetailRecord) -> BusinessHoursPlaceResponse:
    return BusinessHoursPlaceResponse(
        kakao_place_id=detail.kakao_place_id,
        place_url=detail.place_url,
        place_name=detail.place_name,
        business_hours=detail.business_hours,
        business_hours_raw=detail.business_hours_raw,
        business_hours_status=detail.business_hours_status,
        business_hours_fetched_at=detail.business_hours_fetched_at,
        business_hours_expires_at=detail.business_hours_expires_at,
        business_hours_source=detail.business_hours_source,
        business_hours_job_id=detail.business_hours_job_id,
        last_error=detail.last_error,
        created_at=detail.created_at,
        updated_at=detail.updated_at,
        version=detail.version,
    )
