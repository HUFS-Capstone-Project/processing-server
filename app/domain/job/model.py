from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class ExtractionCertainty(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(slots=True)
class ExtractionResult:
    store_name: str | None
    address: str | None
    store_name_evidence: str | None
    address_evidence: str | None
    certainty: ExtractionCertainty


@dataclass(slots=True)
class JobRecord:
    job_id: UUID
    room_id: UUID
    source_url: str
    status: JobStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class JobResultRecord:
    job_id: UUID
    caption: str | None
    instagram_meta: dict[str, Any] | None
    extraction_result: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class ExtractedCandidate:
    keyword: str
    source_keyword: str
    source_sentence: str
    raw_candidate: str


@dataclass(slots=True)
class PlaceCandidate:
    place_name: str
    road_address: str | None
    address: str | None
    category: str | None
    kakao_place_id: str
    confidence: float
    source_keyword: str
    source_sentence: str
    raw_candidate: str


@dataclass(slots=True)
class CrawlArtifact:
    url: str
    html: str | None
    text: str
    media_type: str | None
    caption: str | None
    instagram_meta: dict[str, Any] | None


def as_place_dict(place: PlaceCandidate) -> dict[str, Any]:
    return {
        "place_name": place.place_name,
        "road_address": place.road_address,
        "address": place.address,
        "category": place.category,
        "kakao_place_id": place.kakao_place_id,
        "confidence": round(place.confidence, 4),
        "source_keyword": place.source_keyword,
        "source_sentence": place.source_sentence,
        "raw_candidate": place.raw_candidate,
    }


def as_candidate_dict(candidate: ExtractedCandidate) -> dict[str, Any]:
    return {
        "keyword": candidate.keyword,
        "source_keyword": candidate.source_keyword,
        "source_sentence": candidate.source_sentence,
        "raw_candidate": candidate.raw_candidate,
    }


def as_extraction_result_dict(result: ExtractionResult) -> dict[str, Any]:
    return {
        "store_name": result.store_name,
        "address": result.address,
        "store_name_evidence": result.store_name_evidence,
        "address_evidence": result.address_evidence,
        "certainty": result.certainty.value,
    }
