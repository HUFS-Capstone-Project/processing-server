from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

from app.domain.job.model import ExtractionCertainty, ExtractionResult


class ExtractionLLMResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    store_name: str | None = None
    address: str | None = None
    store_name_evidence: str | None = None
    address_evidence: str | None = None
    certainty: Literal["high", "medium", "low"] | None = None

    @field_validator(
        "store_name",
        "address",
        "store_name_evidence",
        "address_evidence",
        mode="before",
    )
    @classmethod
    def normalize_optional_string(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @field_validator("certainty", mode="before")
    @classmethod
    def normalize_certainty(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip().lower()
            return stripped or None
        return value

    def to_domain(self) -> ExtractionResult:
        return ExtractionResult(
            store_name=self.store_name,
            address=self.address,
            store_name_evidence=self.store_name_evidence,
            address_evidence=self.address_evidence,
            certainty=ExtractionCertainty(self.certainty or "low"),
        )
