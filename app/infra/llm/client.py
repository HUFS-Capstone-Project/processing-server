from __future__ import annotations

import json
import re
from typing import Any

import httpx
from pydantic import ValidationError

from app.core.config import Settings
from app.domain.job import ExtractionResult
from app.schemas.extraction import ExtractionLLMResponse

EXTRACTION_SYSTEM_PROMPT_TEMPLATE_V1 = (
    "You extract place/store information from Korean social media captions. "
    "Return only one JSON object with these exact top-level keys: store_name, "
    "address, store_name_evidence, address_evidence, certainty, places. "
    "places must be an array of objects. Each place object must have these exact "
    "keys: store_name, address, store_name_evidence, address_evidence, certainty. "
    "Extract every distinct place/store/brand that appears to be a visitable local "
    "business, up to {max_candidates} places, preserving caption order. Captions "
    "may contain numbered lists such as 1, 2, circled numbers, or sections such as "
    "brand information, store information, or place information. When a place name "
    "line is followed by an address line, pair them together. Address lines often "
    "start with map-pin markers, address/location labels, or Korean address units "
    "such as city, gu, gun, dong, eup, myeon, ri, ga, ro, or gil. First inspect "
    "hashtags before choosing descriptive category phrases. A hashtag can be a real "
    "store name, for example #StoreName; prioritize it when it appears on the same "
    "line as a map-pin/location marker or near an address, hours, menu, or phone "
    "number. For captions like '📍Guri Gyomun-dong #JukdongSikdang' followed by an "
    "address, extract 'JukdongSikdang' as store_name and pair it with that address. "
    "Prefer specific proper-noun hashtags over generic descriptive phrases such as "
    "old restaurant, pork cutlet restaurant, cafe, dessert shop, hot place, or good "
    "restaurant. Do not extract generic regional/category/promotional "
    "hashtags such as Seoul cafe, Yeonnam cafe, dessert, hot place, date course, "
    "travel, recommendation, or account handles as store names. If a store name is "
    "taken from a hashtag, remove the leading # in store_name but keep the original "
    "hashtag substring in store_name_evidence. Do not invent missing values. Use "
    "null when unknown. Evidence values must be exact substrings copied from the "
    "input caption. certainty must be one of high, medium, or low. The top-level "
    "legacy fields store_name, address, store_name_evidence, address_evidence, and "
    "certainty must mirror the first item in places, or null/low when places is "
    "empty. If no place is found, return places as an empty array. Do not include "
    "explanations, Markdown, or any text outside the JSON object."
)

EXTRACTION_SYSTEM_PROMPT_TEMPLATE_V2 = (
    "You extract visitable place/store information from Korean social media captions. "
    "Return only one JSON object with these exact top-level keys: store_name, address, "
    "store_name_evidence, address_evidence, certainty, places. places must be an array "
    "of objects. Each place object must have these exact keys: store_name, address, "
    "store_name_evidence, address_evidence, certainty. Extract every distinct "
    "place/store/brand that appears to be a visitable local business, up to "
    "{max_candidates} places, preserving caption order. "
    "Extraction priority: "
    "1. Highest priority: explicit place markers. Treat the text immediately after "
    "markers such as '📍', '📌위치 :', '위치 :', '상호명 :', '매장명 :', '가게 :', "
    "or '장소 :' as the place name. If the line has 'PLACE_NAME (ADDRESS)', extract "
    "PLACE_NAME as store_name and ADDRESS as address. "
    "2. If a place marker line is followed by an address line, pair them together. "
    "3. If a hashtag is near an address, hours, menu list, phone number, or place "
    "marker, it may be the store name. Prefer specific proper-noun hashtags, but do "
    "not use generic hashtags. "
    "4. Preserve full branch/store names exactly as written. Keep suffixes such as "
    "본점, 직영점, 성수점, 연신내점, 강남점, 용산 아이파크몰점, 마곡본점. "
    "5. Do not translate, romanize, shorten, or normalize Korean place names. Copy "
    "the exact Korean text from the caption whenever possible. "
    "Never extract these as store_name: menu names such as 우동, 면발, 북어백짬뽕, "
    "직화마라탕, 감자탕, 치즈케이크, 카피바라푸딩; category or title phrases such "
    "as 노포, 이모카세, 술집, 맛집, 카페, 디저트맛집, 찜질방, 야장맛집; "
    "region/category phrases such as 성수맛집, 삼각지맛집, 방이동, 신용산 해산물집; "
    "campaign/prize/event text; account handles. If both a descriptive title and an "
    "explicit place marker exist, always choose the explicit place marker. "
    "Examples: '면발 하나로... 📍우동키노야 신용산본점 서울 용산구...' -> "
    "우동키노야 신용산본점; '이게 카피바라야... 📍 썸머러너' -> 썸머러너, not "
    "카피바라; '📌위치 : 중화객잔수 강남점 (서울 강남구 강남대로66길 11)' -> "
    "중화객잔수 강남점, not 북어백짬뽕; '📍 나침반 연신내점' -> 나침반 연신내점, "
    "not 나침반; '📍 토끼정 용산 아이파크몰점' -> 토끼정 용산 아이파크몰점, "
    "not 토끼정; '📌상호명 : 혼신꼬치 본점' -> 혼신꼬치 본점. "
    "Address lines often start with map-pin markers, address/location labels, or "
    "Korean address units such as city, gu, gun, dong, eup, myeon, ri, ga, ro, or "
    "gil. Do not invent missing values. Use null when unknown. Evidence values must "
    "be exact substrings copied from the input caption. certainty must be one of "
    "high, medium, or low. The top-level legacy fields store_name, address, "
    "store_name_evidence, address_evidence, and certainty must mirror the first item "
    "in places, or null/low when places is empty. If no place is found, return "
    "places as an empty array. Do not include explanations, Markdown, or any text "
    "outside the JSON object."
)

EXTRACTION_SYSTEM_PROMPT_TEMPLATE = EXTRACTION_SYSTEM_PROMPT_TEMPLATE_V2


def build_extraction_system_prompt(max_candidates: int) -> str:
    return EXTRACTION_SYSTEM_PROMPT_TEMPLATE.format(
        max_candidates=max(1, max_candidates),
    )


EXTRACTION_SYSTEM_PROMPT = build_extraction_system_prompt(12)


class HFExtractionError(Exception):
    pass


class HFExtractionClient:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._settings = settings
        self._transport = transport

    async def extract(
        self,
        *,
        text: str,
        source_url: str,
        media_type: str | None,
    ) -> ExtractionResult | None:
        if not text.strip():
            return None
        if not self._settings.hf_extraction_endpoint_url:
            raise HFExtractionError("HF extraction endpoint URL is empty")
        if not self._settings.hf_extraction_api_token:
            raise HFExtractionError("HF extraction API token is empty")

        payload = self._build_payload(
            text=text,
            source_url=source_url,
            media_type=media_type,
        )
        headers = {
            "Authorization": f"Bearer {self._settings.hf_extraction_api_token}",
            "Content-Type": "application/json",
        }
        timeout = httpx.Timeout(self._settings.hf_extraction_timeout_seconds)

        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                transport=self._transport,
            ) as client:
                response = await client.post(
                    self._settings.hf_extraction_endpoint_url,
                    headers=headers,
                    json=payload,
                )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise HFExtractionError(str(exc)) from exc

        if response.status_code >= 400:
            raise HFExtractionError(f"HF request failed ({response.status_code})")

        try:
            response_payload = response.json()
        except json.JSONDecodeError as exc:
            raise HFExtractionError("HF response is not valid JSON") from exc

        generated_text = extract_text_from_hf_payload(response_payload)
        generated_json = extract_json_object(generated_text)

        try:
            result = ExtractionLLMResponse.model_validate(generated_json).to_domain()
        except ValidationError as exc:
            raise HFExtractionError("HF response failed schema validation") from exc
        return self._limit_places(result)

    def _build_payload(
        self,
        *,
        text: str,
        source_url: str,
        media_type: str | None,
    ) -> dict[str, Any]:
        _ = source_url, media_type
        return {
            "model": self._settings.hf_extraction_model_name,
            "messages": [
                {
                    "role": "system",
                    "content": build_extraction_system_prompt(
                        self._settings.extraction_max_candidates,
                    ),
                },
                {"role": "user", "content": text},
            ],
            "temperature": 0.0,
            "max_tokens": self._settings.hf_extraction_max_new_tokens,
        }

    def _limit_places(self, result: ExtractionResult) -> ExtractionResult:
        max_places = max(1, self._settings.extraction_max_candidates)
        if len(result.places) <= max_places:
            return result

        result.places = result.places[:max_places]
        first_place = result.places[0]
        result.store_name = first_place.store_name
        result.address = first_place.address
        result.store_name_evidence = first_place.store_name_evidence
        result.address_evidence = first_place.address_evidence
        result.certainty = first_place.certainty
        return result


def extract_text_from_hf_payload(payload: Any) -> str:
    if isinstance(payload, str):
        return payload

    if isinstance(payload, list):
        if not payload:
            raise HFExtractionError("HF response list is empty")
        return extract_text_from_hf_payload(payload[0])

    if not isinstance(payload, dict):
        raise HFExtractionError("HF response has unsupported shape")

    generated_text = payload.get("generated_text")
    if isinstance(generated_text, str):
        return generated_text

    output = payload.get("output") or payload.get("outputs")
    if isinstance(output, str):
        return output

    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0]
        if isinstance(choice, dict):
            message = choice.get("message")
            if isinstance(message, dict) and isinstance(message.get("content"), str):
                return message["content"]
            if isinstance(choice.get("text"), str):
                return choice["text"]

    raise HFExtractionError("HF response does not contain generated text")


def extract_json_object(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        raise HFExtractionError("Generated text is empty")

    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", raw, re.DOTALL | re.IGNORECASE)
    if fenced:
        raw = fenced.group(1).strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end <= start:
            raise HFExtractionError("Generated text does not contain a JSON object") from None
        try:
            parsed = json.loads(raw[start : end + 1])
        except json.JSONDecodeError as exc:
            raise HFExtractionError("Generated text contains invalid JSON") from exc

    if not isinstance(parsed, dict):
        raise HFExtractionError("Generated JSON is not an object")
    return parsed
