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
from app.infra.llm.client import build_extraction_system_prompt


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _settings() -> Settings:
    return Settings(
        hf_extraction_endpoint_url="https://example.test/hf",
        hf_extraction_api_token="test-token",
        hf_extraction_max_new_tokens=1024,
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
    assert len(result.places) == 1
    assert result.places[0].store_name == "Common Mansion"
    assert seen_requests[0]["messages"][1]["content"] == (
        "Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul"
    )
    assert seen_requests[0]["temperature"] == 0.0
    assert seen_requests[0]["max_tokens"] == 1024


def test_hf_extraction_client_retries_transient_http_failure() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"error": "busy"})
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
            text="Common Mansion",
            source_url="https://www.instagram.com/reel/example/",
            media_type="reel",
        )
    )

    assert result is not None
    assert result.store_name == "Common Mansion"
    assert calls == 2


def test_hf_extraction_client_accepts_long_realistic_caption() -> None:
    long_caption = """실제 광화문 직장인 지인이 여기가 최고라고 소개해줘서 알게 된 집

실내 분위기 너무 좋았던 브런치 맛집 커먼맨션 입니다

샌드위치에 샐러드 파스타 이렇게 3종류로 크게 나눌 수 있는데 샌드위치 먹고 있으면 샌드위치 전문점인 거 같고

샐러드 먹으면 샐러드 파스타면 파스타

모든 메뉴가 전문점 수준으로 너무 맛있어서 정말 대만족했던 집 입니다

광화문 직장인 상권이다보니 점심시간에 가면 웨이팅이 심해서 못 먹고 올 수 있으니까

방문 예정이시면 꼭 예약을 미리 하고 가시는 걸 추천 드릴게요

실제 근처 직장인이시라면 점심 혹은 미팅 잡기도 정말 좋은 곳 일

거 같아요

• 커먼맨션

서울 종로구 신문로2가 1-102
10:00 - 21:00
20:00 라스트오더"""
    seen_requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append(json.loads(request.content.decode("utf-8")))
        return httpx.Response(
            200,
            json={
                "generated_text": json.dumps(
                    {
                        "store_name": "커먼맨션",
                        "address": "서울 종로구 신문로2가 1-102",
                        "store_name_evidence": "커먼맨션",
                        "address_evidence": "서울 종로구 신문로2가 1-102",
                        "certainty": "high",
                    },
                    ensure_ascii=False,
                )
            },
        )

    extractor = HFExtractionClient(
        _settings(),
        transport=httpx.MockTransport(handler),
    )

    result = _run(
        extractor.extract(
            text=long_caption,
            source_url="https://www.instagram.com/reel/example/",
            media_type="reel",
        )
    )

    assert result is not None
    assert result.store_name == "커먼맨션"
    assert result.address == "서울 종로구 신문로2가 1-102"
    assert result.certainty is ExtractionCertainty.HIGH
    assert [place.store_name for place in result.places] == ["커먼맨션"]
    assert seen_requests[0]["messages"][1]["content"] == long_caption


def test_hf_extraction_client_returns_multiple_domain_places() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "generated_text": json.dumps(
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
                    },
                    ensure_ascii=False,
                )
            },
        )

    extractor = HFExtractionClient(
        _settings(),
        transport=httpx.MockTransport(handler),
    )

    result = _run(
        extractor.extract(
            text="#플루밍\n서울 마포구 연남로13길 9 1층 101호\n❷ 누크녹",
            source_url="https://www.instagram.com/reel/example/",
            media_type="reel",
        )
    )

    assert result is not None
    assert result.store_name == "플루밍"
    assert result.address == "서울 마포구 연남로13길 9 1층 101호"
    assert [place.store_name for place in result.places] == ["플루밍", "누크녹"]


def test_build_extraction_system_prompt_mentions_hashtag_store_names() -> None:
    prompt = build_extraction_system_prompt(6)

    assert "hashtag" in prompt
    assert "First inspect hashtags" in prompt
    assert "prioritize it when it appears on the same line as a map-pin" in prompt
    assert "Prefer specific proper-noun hashtags" in prompt
    assert "remove the leading #" in prompt
    assert "up to 6 places" in prompt


def test_hf_extraction_client_raises_on_http_error() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
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
    assert calls == 3


@pytest.mark.parametrize("status_code", [400, 401, 402, 403])
def test_hf_extraction_client_does_not_retry_non_transient_http_errors(status_code: int) -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status_code, json={"error": "non retryable"})

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
    assert calls == 1


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
