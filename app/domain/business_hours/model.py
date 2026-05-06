from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID


class BusinessHoursJobStatus(str, Enum):
    PENDING = "PENDING"
    FETCHING = "FETCHING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ENQUEUE_FAILED = "ENQUEUE_FAILED"


class BusinessHoursDetailStatus(str, Enum):
    PENDING = "PENDING"
    FETCHING = "FETCHING"
    SUCCESS = "SUCCESS"
    NOT_FOUND = "NOT_FOUND"
    CRAWL_FAILED = "CRAWL_FAILED"
    PARSE_FAILED = "PARSE_FAILED"
    ENQUEUE_FAILED = "ENQUEUE_FAILED"


@dataclass(slots=True)
class BusinessHoursJobRecord:
    job_id: UUID
    kakao_place_id: str
    place_url: str
    status: BusinessHoursJobStatus
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class BusinessHoursDetailRecord:
    kakao_place_id: str
    place_url: str
    place_name: str | None
    business_hours: dict[str, Any] | None
    business_hours_raw: str | None
    business_hours_status: BusinessHoursDetailStatus
    business_hours_fetched_at: datetime | None
    business_hours_expires_at: datetime | None
    business_hours_source: str | None
    business_hours_job_id: UUID | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    version: int


@dataclass(slots=True)
class BusinessHoursCreateOutcome:
    job: BusinessHoursJobRecord | None
    detail: BusinessHoursDetailRecord
    created: bool
    enqueued: bool
    cache_hit: bool


@dataclass(slots=True)
class BusinessHoursJobSubmission:
    kakao_place_id: str
    place_url: str
    place_name: str | None = None


@dataclass(slots=True)
class BusinessHoursParseResult:
    status: BusinessHoursDetailStatus
    business_hours: dict[str, Any] | None
    raw_text: str | None
    error_message: str | None = None
