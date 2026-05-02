from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.domain.job.model import ExtractedPlace, ExtractionCertainty, ExtractionResult


def _normalize_optional_string(value: object, *, strip_hash: bool = False) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if strip_hash:
            stripped = stripped.lstrip("#").strip()
        return stripped or None
    return value


def _normalize_certainty(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip().lower()
        return stripped or None
    return value


class ExtractedPlaceLLMResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    store_name: str | None = None
    address: str | None = None
    store_name_evidence: str | None = None
    address_evidence: str | None = None
    certainty: Literal["high", "medium", "low"] | None = None

    @field_validator("store_name", mode="before")
    @classmethod
    def normalize_store_name(cls, value: object) -> object:
        return _normalize_optional_string(value, strip_hash=True)

    @field_validator(
        "address",
        "store_name_evidence",
        "address_evidence",
        mode="before",
    )
    @classmethod
    def normalize_optional_string(cls, value: object) -> object:
        return _normalize_optional_string(value)

    @field_validator("certainty", mode="before")
    @classmethod
    def normalize_certainty(cls, value: object) -> object:
        return _normalize_certainty(value)

    def has_content(self) -> bool:
        return any(
            (
                self.store_name,
                self.address,
                self.store_name_evidence,
                self.address_evidence,
            )
        )

    def to_domain(self) -> ExtractedPlace:
        return ExtractedPlace(
            store_name=self.store_name,
            address=self.address,
            store_name_evidence=self.store_name_evidence,
            address_evidence=self.address_evidence,
            certainty=ExtractionCertainty(self.certainty or "low"),
        )


class ExtractionLLMResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    store_name: str | None = None
    address: str | None = None
    store_name_evidence: str | None = None
    address_evidence: str | None = None
    certainty: Literal["high", "medium", "low"] | None = None
    places: list[ExtractedPlaceLLMResponse] = Field(default_factory=list)

    @field_validator("store_name", mode="before")
    @classmethod
    def normalize_store_name(cls, value: object) -> object:
        return _normalize_optional_string(value, strip_hash=True)

    @field_validator(
        "address",
        "store_name_evidence",
        "address_evidence",
        mode="before",
    )
    @classmethod
    def normalize_optional_string(cls, value: object) -> object:
        return _normalize_optional_string(value)

    @field_validator("certainty", mode="before")
    @classmethod
    def normalize_certainty(cls, value: object) -> object:
        return _normalize_certainty(value)

    @field_validator("places", mode="before")
    @classmethod
    def normalize_places(cls, value: object) -> object:
        if value is None:
            return []
        return value

    def to_domain(self) -> ExtractionResult:
        places = [place.to_domain() for place in self.places if place.has_content()]
        if not places and self._has_legacy_content():
            places = [
                ExtractedPlace(
                    store_name=self.store_name,
                    address=self.address,
                    store_name_evidence=self.store_name_evidence,
                    address_evidence=self.address_evidence,
                    certainty=ExtractionCertainty(self.certainty or "low"),
                )
            ]

        first_place = places[0] if places else None
        return ExtractionResult(
            store_name=first_place.store_name if first_place else self.store_name,
            address=first_place.address if first_place else self.address,
            store_name_evidence=(
                first_place.store_name_evidence if first_place else self.store_name_evidence
            ),
            address_evidence=first_place.address_evidence if first_place else self.address_evidence,
            certainty=(
                first_place.certainty
                if first_place
                else ExtractionCertainty(self.certainty or "low")
            ),
            places=places,
        )

    def _has_legacy_content(self) -> bool:
        return any(
            (
                self.store_name,
                self.address,
                self.store_name_evidence,
                self.address_evidence,
            )
        )
