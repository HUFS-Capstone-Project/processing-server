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
    assert len(domain.places) == 1
    assert domain.places[0].store_name == "커먼맨션"


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
    assert len(domain.places) == 1
    assert domain.places[0].address == "서울 종로구 신문로2가 1-102"


def test_llm_response_rejects_unknown_certainty() -> None:
    with pytest.raises(ValidationError):
        ExtractionLLMResponse.model_validate({"certainty": "certain"})


def test_llm_response_accepts_multiple_places_and_hashtag_store_name() -> None:
    response = ExtractionLLMResponse.model_validate(
        {
            "places": [
                {
                    "store_name": "#플루밍",
                    "address": "서울 마포구 연남로13길 9 1층 101호",
                    "store_name_evidence": "#플루밍",
                    "address_evidence": "서울 마포구 연남로13길 9 1층 101호",
                    "certainty": "high",
                },
                {
                    "store_name": "누크녹",
                    "address": "서울 마포구 성미산로 190-31 2층",
                    "store_name_evidence": "❷ 누크녹",
                    "address_evidence": "서울 마포구 성미산로 190-31 2층",
                    "certainty": "high",
                },
            ]
        }
    )

    domain = response.to_domain()

    assert domain.store_name == "플루밍"
    assert domain.address == "서울 마포구 연남로13길 9 1층 101호"
    assert [place.store_name for place in domain.places] == ["플루밍", "누크녹"]
    assert domain.places[0].store_name_evidence == "#플루밍"
