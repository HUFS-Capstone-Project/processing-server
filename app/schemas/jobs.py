from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl

from app.domain.job.model import JobStatus


class CreateJobRequest(BaseModel):
    url: HttpUrl = Field(..., examples=["https://www.instagram.com/reel/abcde/"])
    idempotency_key: str | None = Field(default=None, max_length=128)
    source: Literal["web", "app"] | None = None
    room_id: str | None = Field(default=None, max_length=128)


class CreateJobResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    created_at: datetime
    idempotent_reused: bool


class JobStatusResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    attempt: int
    max_attempts: int
    error_code: str | None
    error_message: str | None
    created_at: datetime
    queued_at: datetime | None
    processing_started_at: datetime | None
    completed_at: datetime | None


class PlaceResponse(BaseModel):
    place_name: str
    road_address: str | None
    address: str | None
    category: str | None
    kakao_place_id: str
    confidence: float
    source_keyword: str
    source_sentence: str
    raw_candidate: str


class JobResultResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    caption: str | None
    media_type: str | None
    instagram_meta: dict | None
    raw_candidates: list[dict]
    places: list[PlaceResponse]
    error_code: str | None
    error_message: str | None
    completed_at: datetime | None


class ApiErrorResponse(BaseModel):
    code: str
    message: str
