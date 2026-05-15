from __future__ import annotations

from dataclasses import dataclass, field
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
class ExtractedPlace:
    store_name: str | None
    address: str | None
    store_name_evidence: str | None
    address_evidence: str | None
    certainty: ExtractionCertainty


@dataclass(slots=True)
class ExtractionResult:
    store_name: str | None
    address: str | None
    store_name_evidence: str | None
    address_evidence: str | None
    certainty: ExtractionCertainty
    places: list[ExtractedPlace] = field(default_factory=list)


@dataclass(slots=True)
class JobRecord:
    job_id: UUID
    room_id: UUID
    source_url: str
    status: JobStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    attempt_count: int = 0
    max_attempts: int = 3
    error_code: str | None = None
    next_retry_at: datetime | None = None
    processing_started_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    failed_at: datetime | None = None
    completed_at: datetime | None = None
    normalized_source_url: str | None = None


@dataclass(slots=True)
class JobResultRecord:
    job_id: UUID
    caption: str | None
    instagram_meta: dict[str, Any] | None
    extraction_result: dict[str, Any] | None
    place_candidates: list[dict[str, Any]]
    resolved_places: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True, init=False)
class PlaceSearchQuery:
    query: str
    evidence_text: str
    original_text: str

    def __init__(
        self,
        query: str | None = None,
        evidence_text: str | None = None,
        original_text: str | None = None,
    ) -> None:
        self.query = query or ""
        self.evidence_text = evidence_text or self.query
        self.original_text = original_text or self.query


@dataclass(slots=True, init=False)
class PlaceCandidate:
    kakao_place_id: str
    place_name: str
    category_name: str | None
    category_group_code: str | None
    category_group_name: str | None
    phone: str | None
    address_name: str | None
    road_address_name: str | None
    x: str | None
    y: str | None
    place_url: str | None
    confidence: float
    query: str
    evidence_text: str
    original_text: str

    def __init__(
        self,
        *,
        kakao_place_id: str,
        place_name: str,
        category_name: str | None,
        category_group_code: str | None,
        category_group_name: str | None,
        phone: str | None,
        address_name: str | None,
        road_address_name: str | None,
        x: str | None,
        y: str | None,
        place_url: str | None,
        confidence: float,
        query: str | None = None,
        evidence_text: str | None = None,
        original_text: str | None = None,
    ) -> None:
        self.kakao_place_id = kakao_place_id
        self.place_name = place_name
        self.category_name = category_name
        self.category_group_code = category_group_code
        self.category_group_name = category_group_name
        self.phone = phone
        self.address_name = address_name
        self.road_address_name = road_address_name
        self.x = x
        self.y = y
        self.place_url = place_url
        self.confidence = confidence
        self.query = query or place_name
        self.evidence_text = evidence_text or self.query
        self.original_text = original_text or self.query


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
        "kakao_place_id": place.kakao_place_id,
        "place_name": place.place_name,
        "category_name": place.category_name,
        "category_group_code": place.category_group_code,
        "category_group_name": place.category_group_name,
        "phone": place.phone,
        "address_name": place.address_name,
        "road_address_name": place.road_address_name,
        "x": place.x,
        "y": place.y,
        "place_url": place.place_url,
        "confidence": round(place.confidence, 4),
        "query": place.query,
        "evidence_text": place.evidence_text,
        "original_text": place.original_text,
    }


def as_candidate_dict(candidate: PlaceSearchQuery) -> dict[str, Any]:
    return {
        "query": candidate.query,
        "evidence_text": candidate.evidence_text,
        "original_text": candidate.original_text,
    }


def as_extracted_place_dict(place: ExtractedPlace) -> dict[str, Any]:
    return {
        "store_name": place.store_name,
        "address": place.address,
        "store_name_evidence": place.store_name_evidence,
        "address_evidence": place.address_evidence,
        "certainty": place.certainty.value,
    }


def extracted_places_from_result(result: ExtractionResult) -> list[ExtractedPlace]:
    if result.places:
        return result.places
    if not any(
        (
            result.store_name,
            result.address,
            result.store_name_evidence,
            result.address_evidence,
        )
    ):
        return []
    return [
        ExtractedPlace(
            store_name=result.store_name,
            address=result.address,
            store_name_evidence=result.store_name_evidence,
            address_evidence=result.address_evidence,
            certainty=result.certainty,
        )
    ]


def as_extraction_result_dict(result: ExtractionResult) -> dict[str, Any]:
    return {
        "store_name": result.store_name,
        "address": result.address,
        "store_name_evidence": result.store_name_evidence,
        "address_evidence": result.address_evidence,
        "certainty": result.certainty.value,
        "places": [
            as_extracted_place_dict(place)
            for place in extracted_places_from_result(result)
        ],
    }
