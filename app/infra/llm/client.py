from __future__ import annotations

import asyncio
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
    "of objects with the same exact keys except places. Extract every distinct "
    "visitable local place/store/brand, up to {max_candidates} places, preserving "
    "caption order. "
    "Priority rules: "
    "1. Prefer explicit place markers such as 📍, 📌위치, 위치, 상호명, 매장명, 가게, "
    "or 장소. Text after the marker is usually the store_name. If a marker line is "
    "followed by an address line, pair them. If a line has 'PLACE_NAME (ADDRESS)', "
    "extract PLACE_NAME as store_name and ADDRESS as address. "
    "2. First inspect hashtags before choosing descriptive category phrases. A "
    "specific proper-noun hashtag near an address, hours, menu, phone number, or "
    "place marker may be the store_name; prioritize it when it appears on the same "
    "line as a map-pin. Prefer specific proper-noun hashtags over generic hashtags, "
    "and remove the leading # when using one as store_name. "
    "3. Preserve full Korean place names exactly as written, including branch or "
    "store suffixes such as 본점, 직영점, 성수점, 강남점, and 용산 아이파크몰점. Do not "
    "translate, romanize, shorten, or normalize names. "
    "4. Never extract menu names, categories, title phrases, region tags, prizes, "
    "account handles, or promotional phrases as store_name when a real place marker "
    "or proper noun exists. "
    "Examples: '📌위치 : 진담옥 감자탕 (서울 강남구 선릉로86길 12 2층)' -> "
    "store_name '진담옥 감자탕', address '서울 강남구 선릉로86길 12 2층'. "
    "'이게 카피바라야... 📍 썸머러너' -> store_name '썸머러너', not '카피바라'. "
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
        max_attempts = max(1, self._settings.hf_extraction_max_attempts)
        last_error: HFExtractionError | None = None

        async with httpx.AsyncClient(
            timeout=timeout,
            transport=self._transport,
        ) as client:
            for attempt in range(1, max_attempts + 1):
                try:
                    response = await client.post(
                        self._settings.hf_extraction_endpoint_url,
                        headers=headers,
                        json=payload,
                    )
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    last_error = HFExtractionError(str(exc) or exc.__class__.__name__)
                    if attempt >= max_attempts:
                        raise last_error from exc
                    await self._sleep_before_retry(attempt)
                    continue

                if response.status_code >= 400:
                    error = HFExtractionError(f"HF request failed ({response.status_code})")
                    if not _is_retryable_status(response.status_code) or attempt >= max_attempts:
                        raise error
                    last_error = error
                    await self._sleep_before_retry(attempt)
                    continue

                return self._parse_response(response)

        if last_error is not None:
            raise last_error
        raise HFExtractionError("HF extraction failed")

    def _parse_response(self, response: httpx.Response) -> ExtractionResult:
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

    async def _sleep_before_retry(self, attempt: int) -> None:
        base_seconds = max(0.0, self._settings.hf_extraction_retry_base_seconds)
        if base_seconds <= 0:
            return
        multiplier = max(1.0, self._settings.hf_extraction_retry_backoff_multiplier)
        await asyncio.sleep(base_seconds * (multiplier ** max(0, attempt - 1)))

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


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


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
