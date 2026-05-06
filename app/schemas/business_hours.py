from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.domain.business_hours import (
    BusinessHoursDetailStatus,
    BusinessHoursJobStatus,
)


class CreateBusinessHoursJobRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    kakao_place_id: str = Field(..., alias="kakaoPlaceId")
    place_url: HttpUrl = Field(..., alias="placeUrl")
    place_name: str | None = Field(default=None, alias="placeName")


class BusinessHoursJobResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: UUID = Field(..., alias="jobId")
    kakao_place_id: str = Field(..., alias="kakaoPlaceId")
    place_url: str = Field(..., alias="placeUrl")
    status: BusinessHoursJobStatus
    error_code: str | None = Field(default=None, alias="errorCode")
    error_message: str | None = Field(default=None, alias="errorMessage")
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")


class BusinessHoursPlaceResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    kakao_place_id: str = Field(..., alias="kakaoPlaceId")
    place_url: str = Field(..., alias="placeUrl")
    place_name: str | None = Field(default=None, alias="placeName")
    business_hours: dict[str, Any] | None = Field(default=None, alias="businessHours")
    business_hours_raw: str | None = Field(default=None, alias="businessHoursRaw")
    business_hours_status: BusinessHoursDetailStatus = Field(..., alias="businessHoursStatus")
    business_hours_fetched_at: datetime | None = Field(default=None, alias="businessHoursFetchedAt")
    business_hours_expires_at: datetime | None = Field(default=None, alias="businessHoursExpiresAt")
    business_hours_source: str | None = Field(default=None, alias="businessHoursSource")
    business_hours_job_id: UUID | None = Field(default=None, alias="businessHoursJobId")
    last_error: str | None = Field(default=None, alias="lastError")
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")
    version: int


class CreateBusinessHoursJobResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job: BusinessHoursJobResponse | None
    place: BusinessHoursPlaceResponse
    created: bool
    enqueued: bool
    cache_hit: bool = Field(..., alias="cacheHit")


class BusinessHoursJobStatusResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job: BusinessHoursJobResponse
    place: BusinessHoursPlaceResponse | None = None
