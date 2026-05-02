from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl

from app.domain.job.model import JobStatus


class ExtractedPlaceResponse(BaseModel):
    store_name: str | None
    address: str | None
    store_name_evidence: str | None
    address_evidence: str | None
    certainty: Literal["high", "medium", "low"]


class ExtractionResultResponse(BaseModel):
    store_name: str | None
    address: str | None
    store_name_evidence: str | None
    address_evidence: str | None
    certainty: Literal["high", "medium", "low"]
    places: list[ExtractedPlaceResponse] = Field(default_factory=list)


class PlaceCandidateResponse(BaseModel):
    kakao_place_id: str
    place_name: str
    category_name: str | None = None
    category_group_code: str | None = None
    category_group_name: str | None = None
    phone: str | None = None
    address_name: str | None = None
    road_address_name: str | None = None
    x: str | None = None
    y: str | None = None
    place_url: str | None = None
    confidence: float
    source_keyword: str | None = None
    source_sentence: str | None = None
    raw_candidate: str | None = None


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
    extraction_result: ExtractionResultResponse | None = None
    place_candidates: list[PlaceCandidateResponse] = Field(default_factory=list)
    selected_place: PlaceCandidateResponse | None = None
    selected_places: list[PlaceCandidateResponse] = Field(default_factory=list)
    error_message: str | None
    updated_at: datetime


class ApiErrorResponse(BaseModel):
    code: str
    message: str
