from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl

from app.domain.job.model import JobStatus


class InstagramMetaResponse(BaseModel):
    like_count: int | None = None
    comment_count: int | None = None


class ResolvedPlaceResponse(BaseModel):
    kakao_place_id: str
    place_name: str
    address: str | None = None
    road_address: str | None = None
    longitude: float | None = None
    latitude: float | None = None
    category_name: str | None = None
    category_group_code: str | None = None
    place_url: str | None = None
    phone: str | None = None


class CreateJobRequest(BaseModel):
    url: HttpUrl = Field(..., examples=["https://www.instagram.com/reel/abcde/"])
    room_id: UUID


class CreateJobResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    source_url: str
    source: Literal["instagram", "web"] | None
    created_at: datetime


class JobStatusResponse(BaseModel):
    job_id: UUID
    room_id: UUID
    source_url: str
    source: Literal["instagram", "web"] | None
    status: JobStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime


class JobResultResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    caption_raw: str | None
    instagram_meta: InstagramMetaResponse | None = None
    resolved_places: list[ResolvedPlaceResponse] = Field(default_factory=list)
    error_code: str | None = None
    error_message: str | None = None


class JobDebugResultResponse(BaseModel):
    job_id: UUID
    room_id: UUID
    source_url: str
    normalized_source_url: str | None = None
    source: Literal["instagram", "web"] | None
    status: JobStatus
    attempt_count: int
    max_attempts: int
    error_code: str | None = None
    error_message: str | None = None
    next_retry_at: datetime | None = None
    processing_started_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    failed_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    caption_raw: str | None = None
    instagram_meta: dict[str, Any] | None = None
    extraction_result: dict[str, Any] | None = None
    place_candidates: list[dict[str, Any]] = Field(default_factory=list)
    resolved_places: list[dict[str, Any]] = Field(default_factory=list)


class ApiErrorResponse(BaseModel):
    code: str
    message: str

