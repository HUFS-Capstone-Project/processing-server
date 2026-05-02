from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pytest

from app.core.config import Settings
from app.domain.job import (
    CrawlArtifact,
    ExtractedCandidate,
    as_extraction_result_dict,
    as_place_dict,
)
from app.infra.kakao import KakaoLocalClient
from app.infra.llm import HFExtractionClient
from app.worker.processor import JobProcessor

if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


RUN_LIVE_TESTS = os.getenv("RUN_LIVE_HF_KAKAO_TESTS") == "1"
ARTIFACT_PATH = Path("artifacts") / "hf_kakao_pipeline_live_results.json"


MULTI_PLACE_CAPTION = """🍰먹기 전에 한 번 더 고민하게 되는 순간
서울에서 만나는 비주얼 디저트 카페들🍓

디저트는 맛도 중요하지만,
요즘은 눈부터 만족시켜주는 게 먼저인 느낌

접시에 담긴 색감, 디테일 하나하나
사진부터 찍게 되는 디저트들📸

보기만 해도 기분 좋아지는 디저트들로
식후를 더 길게 만들 카페들 모아봤어요🍰

🗒️ 브랜드 정보
❶ 플루밍
📍서울 마포구 연남로13길 9 1층 101호
🍰토~수 12:00 ~ 19:30
🍰목~금 12:00 ~ 21:00
🍰매주 월, 화 정기휴무

❷ 누크녹
📍서울 마포구 성미산로 190-31 2층
🍰12:30 ~ 20:30
🍰매주 월 정기휴무

❸ 예챠
📍서울 마포구 망원로7길 31-18 1층 102호
🍰12:00 ~ 19:00
🍰매주 월 정기휴무

❹ 라뚜셩트
📍서울 서초구 방배로25길 50 1층
🍰월~목 08:00 ~ 19:00
🍰금~일 08:00 ~ 20:00

❺ 코이크
📍서울 마포구 동교로39길 8 1-2층
🍰월~목 12:00 ~ 20:30
🍰금~일 12:00 ~ 21:30

❻ 카페토요
📍서울 영등포구 도림로 436-7 1층
🍰12:00 ~ 20:00
🍰매주 월 정기휴무

이미지 | 각 브랜드 채널

요즘 감성 핫플 한눈에 보고 싶다면? @eateat.mag
데이트·여행 등 전국 ‘핫한 정보’ 필요하면? @eateat.mag
놓치면 후회할 핫플 리스트 @eateat.mag

#서울디저트 #서울카페 #연남카페 #망원카페"""


SINGLE_PLACE_CAPTION = """실제 광화문 직장인 지인이 여기가 최고라고 소개해줘서 알게 된 집

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


@dataclass(frozen=True)
class LivePipelineCase:
    case_id: str
    caption: str
    expected_place_names: list[str]


class RecordingKakaoLocalClient:
    def __init__(self, client: KakaoLocalClient, settings: Settings) -> None:
        self._client = client
        self._settings = settings
        self.calls: list[dict[str, object]] = []

    async def search_places(
        self,
        candidate: ExtractedCandidate,
        location_hints: list[str],
    ):
        started = perf_counter()
        result = await self._client.search_places(candidate, location_hints)
        elapsed_ms = int((perf_counter() - started) * 1000)
        qualified = [
            place
            for place in result.places
            if place.confidence >= self._settings.kakao_min_place_confidence
        ]
        self.calls.append(
            {
                "keyword": candidate.keyword,
                "location_hints": location_hints,
                "elapsed_ms": elapsed_ms,
                "returned_count": len(result.places),
                "qualified_count": len(qualified),
                "returned_places": [as_place_dict(place) for place in result.places],
                "qualified_places": [as_place_dict(place) for place in qualified],
            }
        )
        return result


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _settings_for_live_test() -> Settings:
    configured = Settings()
    settings = Settings(
        hf_extraction_timeout_seconds=60,
        hf_extraction_max_new_tokens=max(2048, configured.hf_extraction_max_new_tokens),
        kakao_timeout_seconds=10,
    )
    missing = []
    if not settings.hf_extraction_endpoint_url:
        missing.append("HF_EXTRACTION_ENDPOINT_URL")
    if not settings.hf_extraction_api_token:
        missing.append("HF_EXTRACTION_API_TOKEN")
    if not settings.kakao_rest_api_key:
        missing.append("KAKAO_REST_API_KEY")
    if missing:
        pytest.skip(f"Live HF/Kakao test credentials are missing: {', '.join(missing)}")
    return settings


def _normalize_place_name(value: str | None) -> str:
    return "".join((value or "").lower().split())


def _contains_place_name(actual_names: list[str | None], expected_name: str) -> bool:
    expected = _normalize_place_name(expected_name)
    return any(expected in _normalize_place_name(actual) for actual in actual_names)


async def _run_live_pipeline_case(
    case: LivePipelineCase,
    settings: Settings,
) -> dict[str, object]:
    extractor = HFExtractionClient(settings)
    kakao = RecordingKakaoLocalClient(KakaoLocalClient(settings), settings)
    processor = JobProcessor(
        repository=None,  # type: ignore[arg-type]
        settings=settings,
        place_search_client=kakao,
    )

    started = perf_counter()
    extraction_result = await extractor.extract(
        text=case.caption,
        source_url=f"https://www.instagram.com/reel/live-{case.case_id}/",
        media_type="reel",
    )
    extraction_elapsed_ms = int((perf_counter() - started) * 1000)

    crawl_artifact = CrawlArtifact(
        url=f"https://www.instagram.com/reel/live-{case.case_id}/",
        html=None,
        text=case.caption,
        media_type="reel",
        caption=case.caption,
        instagram_meta={"caption": case.caption},
    )
    place_candidates, selected_place, selected_places = await processor._enrich_place(
        extraction_result,
        crawl_artifact,
    )

    extraction_dict = as_extraction_result_dict(extraction_result) if extraction_result else None
    extracted_names = [
        place.get("store_name")
        for place in (extraction_dict or {}).get("places", [])
        if isinstance(place, dict)
    ]
    selected_names = [place.get("place_name") for place in selected_places]
    extraction_matches = {
        name: _contains_place_name(extracted_names, name)
        for name in case.expected_place_names
    }
    selected_matches = {
        name: _contains_place_name(selected_names, name)
        for name in case.expected_place_names
    }

    return {
        "case_id": case.case_id,
        "caption": case.caption,
        "expected_place_names": case.expected_place_names,
        "extraction_elapsed_ms": extraction_elapsed_ms,
        "extraction_result": extraction_dict,
        "extracted_names": extracted_names,
        "extraction_matches": extraction_matches,
        "kakao_calls": kakao.calls,
        "place_candidates": place_candidates,
        "place_candidate_count": len(place_candidates),
        "selected_place": selected_place,
        "selected_places": selected_places,
        "selected_place_count": len(selected_places),
        "selected_matches": selected_matches,
    }


@pytest.mark.skipif(
    not RUN_LIVE_TESTS,
    reason="Set RUN_LIVE_HF_KAKAO_TESTS=1 to call live HF and Kakao APIs.",
)
def test_live_hf_extraction_to_kakao_search_writes_artifact() -> None:
    settings = _settings_for_live_test()
    cases = [
        LivePipelineCase(
            case_id="dessert_cafes_multi_place",
            caption=MULTI_PLACE_CAPTION,
            expected_place_names=["플루밍", "누크녹", "예챠", "라뚜셩트", "코이크", "카페토요"],
        ),
        LivePipelineCase(
            case_id="common_mansion_single_place",
            caption=SINGLE_PLACE_CAPTION,
            expected_place_names=["커먼맨션"],
        ),
    ]

    results = _run(_run_all_live_pipeline_cases(cases, settings))
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.write_text(
        json.dumps(
            {
                "settings": {
                    "hf_extraction_endpoint_url_configured": bool(
                        settings.hf_extraction_endpoint_url
                    ),
                    "hf_extraction_model_name": settings.hf_extraction_model_name,
                    "hf_extraction_max_new_tokens": settings.hf_extraction_max_new_tokens,
                    "kakao_base_url": settings.kakao_base_url,
                    "kakao_max_places_per_candidate": settings.kakao_max_places_per_candidate,
                    "kakao_min_place_confidence": settings.kakao_min_place_confidence,
                },
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    failures: list[str] = []
    for result in results:
        case_id = result["case_id"]
        error = result.get("error")
        if error:
            failures.append(f"{case_id} error: {error}")
            continue
        missing_extractions = [
            name
            for name, matched in result["extraction_matches"].items()
            if not matched
        ]
        missing_selections = [
            name
            for name, matched in result["selected_matches"].items()
            if not matched
        ]
        if missing_extractions:
            failures.append(f"{case_id} missing extraction: {missing_extractions}")
        if missing_selections:
            failures.append(f"{case_id} missing selected Kakao match: {missing_selections}")

    assert not failures, f"Live HF/Kakao pipeline mismatches. See {ARTIFACT_PATH}: {failures}"


async def _run_all_live_pipeline_cases(
    cases: list[LivePipelineCase],
    settings: Settings,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for case in cases:
        try:
            results.append(await _run_live_pipeline_case(case, settings))
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "case_id": case.case_id,
                    "caption": case.caption,
                    "expected_place_names": case.expected_place_names,
                    "error": f"{type(exc).__name__}: {exc}",
                    "extraction_result": None,
                    "extracted_names": [],
                    "extraction_matches": {
                        name: False for name in case.expected_place_names
                    },
                    "kakao_calls": [],
                    "place_candidates": [],
                    "place_candidate_count": 0,
                    "selected_place": None,
                    "selected_places": [],
                    "selected_place_count": 0,
                    "selected_matches": {
                        name: False for name in case.expected_place_names
                    },
                }
            )
    return results
