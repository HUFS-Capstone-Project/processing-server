from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.domain.business_hours import (
    BusinessHoursFetchStatus,
    BusinessHoursJobStatus,
)


class CreateBusinessHoursJobRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    kakao_place_id: str = Field(..., alias="kakaoPlaceId")
    place_url: HttpUrl = Field(..., alias="placeUrl")
    place_name: str | None = Field(default=None, alias="placeName")


class BusinessHoursJobResponse(BaseModel):
    job_id: UUID
    status: BusinessHoursJobStatus


class BusinessHoursPlaceResponse(BaseModel):
    kakao_place_id: str
    place_name: str | None = None
    place_url: str
    business_hours_status: BusinessHoursFetchStatus
    business_hours: Any | None = None
    business_hours_fetched_at: datetime | None = None
    business_hours_expires_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None


class CreateBusinessHoursJobResponse(BaseModel):
    cache_hit: bool
    job: BusinessHoursJobResponse | None
    place: BusinessHoursPlaceResponse


class BusinessHoursJobStatusResponse(BaseModel):
    job: BusinessHoursJobResponse
    place: BusinessHoursPlaceResponse | None = None


class BusinessHoursDebugJobResponse(BaseModel):
    job_id: UUID
    kakao_place_id: str
    place_url: str
    status: BusinessHoursJobStatus
    error_code: str | None = None
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime


class BusinessHoursDebugPlaceResponse(BaseModel):
    kakao_place_id: str
    place_url: str
    place_name: str | None = None
    business_hours: Any | None = None
    business_hours_raw: str | None = None
    business_hours_status: BusinessHoursFetchStatus
    business_hours_fetched_at: datetime | None = None
    business_hours_expires_at: datetime | None = None
    business_hours_source: str | None = None
    business_hours_job_id: UUID | None = None
    last_error: str | None = None
    created_at: datetime
    updated_at: datetime
    version: int


class BusinessHoursDebugResultResponse(BaseModel):
    job: BusinessHoursDebugJobResponse
    place: BusinessHoursDebugPlaceResponse | None = None

