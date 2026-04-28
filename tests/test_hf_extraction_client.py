from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from app.core.config import Settings
from app.domain.job import ExtractionCertainty
from app.infra.llm import (
    HFExtractionClient,
    HFExtractionError,
    extract_json_object,
    extract_text_from_hf_payload,
)


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _settings() -> Settings:
    return Settings(
        hf_extraction_endpoint_url="https://example.test/hf",
        hf_extraction_api_token="test-token",
    )


def _response_payload() -> dict[str, object]:
    return {
        "store_name": "Common Mansion",
        "address": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "store_name_evidence": "Common Mansion",
        "address_evidence": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "certainty": "HIGH",
    }


def test_extract_json_object_accepts_fenced_json() -> None:
    parsed = extract_json_object(f"```json\n{json.dumps(_response_payload())}\n```")

    assert parsed["store_name"] == "Common Mansion"


def test_extract_json_object_accepts_text_wrapped_json() -> None:
    parsed = extract_json_object(f"Here is the result:\n{json.dumps(_response_payload())}\nDone.")

    assert parsed["certainty"] == "HIGH"


def test_extract_text_from_hf_payload_accepts_common_shapes() -> None:
    assert extract_text_from_hf_payload({"generated_text": "a"}) == "a"
    assert extract_text_from_hf_payload([{"generated_text": "b"}]) == "b"
    assert extract_text_from_hf_payload({"choices": [{"message": {"content": "c"}}]}) == "c"


def test_hf_extraction_client_returns_domain_result() -> None:
    seen_requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append(json.loads(request.content.decode("utf-8")))
        return httpx.Response(
            200,
            json={"generated_text": json.dumps(_response_payload())},
        )

    extractor = HFExtractionClient(
        _settings(),
        transport=httpx.MockTransport(handler),
    )

    result = _run(
        extractor.extract(
            text="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            source_url="https://www.instagram.com/reel/example/",
            media_type="reel",
        )
    )

    assert result is not None
    assert result.store_name == "Common Mansion"
    assert result.certainty is ExtractionCertainty.HIGH
    assert seen_requests[0]["messages"][1]["content"] == (
        "Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul"
    )
    assert seen_requests[0]["temperature"] == 0.0
    assert seen_requests[0]["max_tokens"] == 512


def test_hf_extraction_client_raises_on_http_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "temporary failure"})

    extractor = HFExtractionClient(
        _settings(),
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(HFExtractionError):
        _run(
            extractor.extract(
                text="Common Mansion",
                source_url="https://example.com/post",
                media_type=None,
            )
        )


def test_hf_extraction_client_raises_on_invalid_schema() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"generated_text": json.dumps({"certainty": "unknown"})},
        )

    extractor = HFExtractionClient(
        _settings(),
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(HFExtractionError):
        _run(
            extractor.extract(
                text="Common Mansion",
                source_url="https://example.com/post",
                media_type=None,
            )
        )


def test_hf_extraction_client_raises_when_endpoint_is_missing() -> None:
    extractor = HFExtractionClient(
        Settings(
            hf_extraction_endpoint_url="",
            hf_extraction_api_token="test-token",
        )
    )

    with pytest.raises(HFExtractionError):
        _run(
            extractor.extract(
                text="Common Mansion",
                source_url="https://example.com/post",
                media_type=None,
            )
        )
