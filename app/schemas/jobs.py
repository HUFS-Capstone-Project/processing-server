from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from app.domain.job.model import JobStatus


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
    original_url: str = Field(
        ...,
        examples=["https://www.instagram.com/reel/abcde/"],
        json_schema_extra={"format": "uri"},
    )
    room_id: UUID


class CreateJobResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    original_url: str = Field(..., description="Original URL submitted by the client.")
    canonical_url: str = Field(
        ...,
        description="Canonical URL used for duplicate detection and link identity.",
    )
    created_at: datetime


class JobStatusResponse(BaseModel):
    job_id: UUID
    room_id: UUID
    original_url: str = Field(..., description="Original URL submitted by the client.")
    canonical_url: str = Field(
        ...,
        description="Canonical URL used for duplicate detection and link identity.",
    )
    status: JobStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime


class JobResultResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    original_url: str = Field(..., description="Original URL submitted by the client.")
    canonical_url: str = Field(
        ...,
        description="Canonical URL used for duplicate detection and link identity.",
    )
    crawl_url: str = Field(
        ...,
        description="URL used by the crawler after source-specific canonicalization.",
    )
    content: CrawledContentResponse | None = None
    link_stats: LinkStatsResponse | None = None
    resolved_places: list[ResolvedPlaceResponse] = Field(default_factory=list)
    error_code: str | None = Field(
        default=None,
        description=(
            "Machine-readable failure code when status=FAILED. Known values include "
            "INSTAGRAM_RATE_LIMITED, EMPTY_INSTAGRAM_CRAWL, RETRYABLE_TIMEOUT."
        ),
        examples=["INSTAGRAM_RATE_LIMITED"],
    )
    error_message: str | None = Field(
        default=None,
        description="Human-readable failure message when status=FAILED.",
    )
    retryable: bool = Field(
        default=False,
        description=(
            "True when the client may retry later after a failed job. "
            "Currently true only for INSTAGRAM_RATE_LIMITED. "
            "Does not indicate worker auto-retry is in progress."
        ),
    )


class CrawledContentResponse(BaseModel):
    source_type: str
    content_text: str = Field(
        ...,
        description=(
            "Normalized source content text. For Instagram, this is the post/reel caption "
            "without likes, comments, account name, or posted date metadata."
        ),
    )
    extraction_method: str | None = None


class LinkStatsResponse(BaseModel):
    like_count: int | None = None
    comment_count: int | None = None
    posted_at: str | None = None


class JobDebugResultResponse(BaseModel):
    job_id: UUID
    room_id: UUID
    original_url: str
    canonical_url: str
    crawl_url: str
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
    content: dict[str, Any] | None = None
    link_stats: dict[str, Any] | None = None
    extraction_result: dict[str, Any] | None = None
    place_candidates: list[dict[str, Any]] = Field(default_factory=list)
    resolved_places: list[dict[str, Any]] = Field(default_factory=list)


