from __future__ import annotations

import asyncio

import httpx
import pytest

from app.core.config import Settings
from app.domain.job import PlaceSearchQuery
from app.infra.kakao import KakaoLocalClient, KakaoNonRetryableError

if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _can_create_event_loop() -> bool:
    try:
        loop = asyncio.new_event_loop()
        loop.close()
        return True
    except OSError:
        return False


EVENT_LOOP_AVAILABLE = _can_create_event_loop()


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _settings() -> Settings:
    return Settings(
        kakao_rest_api_key="test-kakao-key",
        kakao_base_url="https://dapi.kakao.com",
        kakao_max_places_per_candidate=5,
    )


def _candidate() -> PlaceSearchQuery:
    return PlaceSearchQuery(
        query="커먼맨션",
        evidence_text="브런치 맛집 커먼맨션 입니다",
        original_text="커먼맨션",
    )


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_kakao_local_client_maps_place_fields() -> None:
    seen_requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append(request)
        return httpx.Response(
            200,
            json={
                "documents": [
                    {
                        "id": "123",
                        "place_name": "커먼맨션",
                        "category_name": "음식점 > 카페",
                        "category_group_code": "CE7",
                        "category_group_name": "카페",
                        "phone": "02-0000-0000",
                        "address_name": "서울 종로구 신문로2가 1-102",
                        "road_address_name": "서울 종로구 새문안로 1",
                        "x": "126.970000",
                        "y": "37.570000",
                        "place_url": "https://place.map.kakao.com/123",
                    }
                ],
                "meta": {"total_count": 1},
            },
        )

    client = KakaoLocalClient(
        _settings(),
        transport=httpx.MockTransport(handler),
    )

    result = _run(
        client.search_places(
            _candidate(),
            location_hints=["서울 종로구 신문로2가 1-102"],
        )
    )

    assert seen_requests[0].headers["Authorization"] == "KakaoAK test-kakao-key"
    assert seen_requests[0].url.params["query"] == "서울 종로구 신문로2가 1-102 커먼맨션"
    assert seen_requests[0].url.params["size"] == "5"
    place = result.places[0]
    assert place.kakao_place_id == "123"
    assert place.place_name == "커먼맨션"
    assert place.category_name == "음식점 > 카페"
    assert place.category_group_code == "CE7"
    assert place.category_group_name == "카페"
    assert place.address_name == "서울 종로구 신문로2가 1-102"
    assert place.road_address_name == "서울 종로구 새문안로 1"
    assert place.x == "126.970000"
    assert place.y == "37.570000"
    assert place.place_url == "https://place.map.kakao.com/123"
    assert place.confidence > 0


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_kakao_local_client_requires_api_key() -> None:
    client = KakaoLocalClient(Settings(kakao_rest_api_key=""))

    with pytest.raises(KakaoNonRetryableError):
        _run(client.search_places(_candidate(), []))


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_kakao_local_client_boosts_exact_address_match_above_threshold() -> None:
    candidate = PlaceSearchQuery(
        query="중앙시장 오복닭집",
        evidence_text="(2) 중앙시장 오복닭집",
        original_text="중앙시장 오복닭집",
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "documents": [
                    {
                        "id": "456",
                        "place_name": "오복닭집",
                        "category_name": "음식점 > 치킨",
                        "category_group_code": "FD6",
                        "category_group_name": "음식점",
                        "phone": "054-000-0000",
                        "address_name": "경북 경주시 성건동 339-2",
                        "road_address_name": "경북 경주시 금성로 295",
                        "x": "129.0",
                        "y": "35.0",
                        "place_url": "https://place.map.kakao.com/456",
                    }
                ],
                "meta": {"total_count": 1},
            },
        )

    client = KakaoLocalClient(
        _settings(),
        transport=httpx.MockTransport(handler),
    )

    result = _run(
        client.search_places(
            candidate,
            location_hints=["경북 경주시 금성로 295"],
        )
    )

    assert result.places[0].place_name == "오복닭집"
    assert result.places[0].confidence >= 0.7


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_kakao_local_client_deduplicates_address_only_query() -> None:
    seen_requests: list[httpx.Request] = []
    address = "경북 경주시 내남면 포석로 110-32"
    candidate = PlaceSearchQuery(
        query=address,
        evidence_text="(4) 수뢰뫼",
        original_text="수뢰뫼",
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append(request)
        return httpx.Response(200, json={"documents": [], "meta": {"total_count": 0}})

    client = KakaoLocalClient(
        _settings(),
        transport=httpx.MockTransport(handler),
    )

    _run(client.search_places(candidate, location_hints=[address]))

    assert seen_requests[0].url.params["query"] == address
