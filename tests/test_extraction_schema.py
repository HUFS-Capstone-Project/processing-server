from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.domain.job import ExtractionCertainty
from app.schemas.extraction import ExtractionLLMResponse


def test_llm_response_normalizes_missing_fields_and_certainty() -> None:
    response = ExtractionLLMResponse.model_validate(
        {
            "store_name": " 커먼맨션 ",
            "certainty": "HIGH",
        }
    )

    assert response.store_name == "커먼맨션"
    assert response.address is None
    assert response.store_name_evidence is None
    assert response.address_evidence is None
    assert response.certainty == "high"

    domain = response.to_domain()

    assert domain.store_name == "커먼맨션"
    assert domain.certainty is ExtractionCertainty.HIGH


def test_llm_response_defaults_missing_certainty_to_low_in_domain() -> None:
    response = ExtractionLLMResponse.model_validate(
        {
            "address": " 서울 종로구 신문로2가 1-102 ",
            "unexpected": "ignored",
        }
    )

    domain = response.to_domain()

    assert response.address == "서울 종로구 신문로2가 1-102"
    assert response.certainty is None
    assert domain.certainty is ExtractionCertainty.LOW


def test_llm_response_rejects_unknown_certainty() -> None:
    with pytest.raises(ValidationError):
        ExtractionLLMResponse.model_validate({"certainty": "certain"})
