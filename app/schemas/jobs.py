from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl

from app.domain.job.model import JobStatus


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
    source_url: str
    source: Literal["instagram", "web"] | None
    status: JobStatus
    caption: str | None
    instagram_meta: dict[str, object] | None
    error_message: str | None
    updated_at: datetime


class ApiErrorResponse(BaseModel):
    code: str
    message: str
