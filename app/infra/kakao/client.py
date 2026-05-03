from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import Settings
from app.domain.job.model import ExtractedCandidate, PlaceCandidate


class KakaoError(Exception):
    code: str = "KAKAO_ERROR"


class KakaoRetryableError(KakaoError):
    code = "KAKAO_RETRYABLE_ERROR"


class KakaoNonRetryableError(KakaoError):
    code = "KAKAO_NON_RETRYABLE_ERROR"


@dataclass(slots=True)
class KakaoSearchResult:
    places: list[PlaceCandidate]
    raw_payload: dict[str, Any]


class KakaoLocalClient:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._settings = settings
        self._headers = {"Authorization": f"KakaoAK {settings.kakao_rest_api_key}"}
        self._transport = transport

    async def search_places(
        self,
        candidate: ExtractedCandidate,
        location_hints: list[str],
    ) -> KakaoSearchResult:
        if not self._settings.kakao_rest_api_key:
            raise KakaoNonRetryableError("KAKAO_REST_API_KEY is empty")

        query = self._build_query(candidate.keyword, location_hints)
        params = {
            "query": query,
            "size": self._settings.kakao_max_places_per_candidate,
            "sort": "accuracy",
        }

        timeout = httpx.Timeout(self._settings.kakao_timeout_seconds)
        url = f"{self._settings.kakao_base_url}/v2/local/search/keyword.json"

        try:
            async with httpx.AsyncClient(timeout=timeout, transport=self._transport) as client:
                response = await client.get(url, params=params, headers=self._headers)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise KakaoRetryableError(str(exc)) from exc

        if response.status_code in {401, 403}:
            raise KakaoNonRetryableError(f"Kakao auth failed ({response.status_code})")
        if response.status_code == 429 or response.status_code >= 500:
            raise KakaoRetryableError(f"Kakao temporary failure ({response.status_code})")
        if response.status_code >= 400:
            raise KakaoNonRetryableError(f"Kakao request failed ({response.status_code})")

        payload = response.json()
        docs = payload.get("documents") or []
        places = self._to_places(candidate, docs, location_hints)
        return KakaoSearchResult(places=places, raw_payload=payload)

    def _build_query(self, keyword: str, location_hints: list[str]) -> str:
        if not location_hints:
            return keyword
        top_hint = location_hints[0]
        if _normalize_address_text(keyword) == _normalize_address_text(top_hint):
            return top_hint
        return f"{top_hint} {keyword}".strip()

    def _to_places(
        self,
        candidate: ExtractedCandidate,
        docs: list[dict[str, Any]],
        location_hints: list[str],
    ) -> list[PlaceCandidate]:
        places: list[PlaceCandidate] = []
        for idx, doc in enumerate(docs):
            place_name = (doc.get("place_name") or "").strip()
            if not place_name:
                continue
            confidence = self._score_place(candidate.keyword, place_name, idx, doc, location_hints)
            places.append(
                PlaceCandidate(
                    kakao_place_id=str(doc.get("id") or ""),
                    place_name=place_name,
                    category_name=(doc.get("category_name") or "").strip() or None,
                    category_group_code=(doc.get("category_group_code") or "").strip() or None,
                    category_group_name=(doc.get("category_group_name") or "").strip() or None,
                    phone=(doc.get("phone") or "").strip() or None,
                    address_name=(doc.get("address_name") or "").strip() or None,
                    road_address_name=(doc.get("road_address_name") or "").strip() or None,
                    x=(doc.get("x") or "").strip() or None,
                    y=(doc.get("y") or "").strip() or None,
                    place_url=(doc.get("place_url") or "").strip() or None,
                    confidence=confidence,
                    source_keyword=candidate.source_keyword,
                    source_sentence=candidate.source_sentence,
                    raw_candidate=candidate.raw_candidate,
                )
            )
        return places

    @staticmethod
    def _score_place(
        keyword: str,
        place_name: str,
        rank: int,
        doc: dict[str, Any],
        location_hints: list[str],
    ) -> float:
        score = 0.35
        if _normalize_place_text(keyword) in _normalize_place_text(place_name):
            score += 0.3
        if rank == 0:
            score += 0.2
        elif rank == 1:
            score += 0.12
        elif rank == 2:
            score += 0.08

        address_blob = " ".join(
            [
                str(doc.get("address_name") or ""),
                str(doc.get("road_address_name") or ""),
            ]
        )
        if _has_exact_address_hint(address_blob, location_hints):
            score += 0.25
        elif any(hint.lower() in address_blob.lower() for hint in location_hints[:2]):
            score += 0.1

        return max(0.0, min(0.99, score))


def _normalize_place_text(value: str) -> str:
    return "".join((value or "").lower().split())


def _normalize_address_text(value: str) -> str:
    return "".join(
        char.lower()
        for char in value or ""
        if char.isalnum()
    )


def _has_exact_address_hint(address_blob: str, location_hints: list[str]) -> bool:
    normalized_address = _normalize_address_text(address_blob)
    if not normalized_address:
        return False

    for hint in location_hints[:2]:
        normalized_hint = _normalize_address_text(hint)
        if not normalized_hint or not any(char.isdigit() for char in normalized_hint):
            continue
        if normalized_hint in normalized_address or normalized_address in normalized_hint:
            return True
    return False
