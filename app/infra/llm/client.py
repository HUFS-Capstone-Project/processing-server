from __future__ import annotations

import json
import re
from typing import Any

import httpx
from pydantic import ValidationError

from app.core.config import Settings
from app.domain.job import ExtractionResult
from app.schemas.extraction import ExtractionLLMResponse

EXTRACTION_SYSTEM_PROMPT = (
    "You extract store information from Korean restaurant social media captions. "
    "Return only one JSON object with these exact keys: store_name, address, "
    "store_name_evidence, address_evidence, certainty. Use null when a value is "
    "unknown. Evidence values must be substrings copied from the input caption. "
    "For store_name, prefer explicit proper nouns near bullets, check marks, "
    "hashtags, or address lines. Do not use generic food, menu, category, or "
    "description phrases as store_name when a more specific proper noun exists. "
    "certainty must be one of high, medium, or low. Do not include explanations, "
    "Markdown, or any text outside the JSON object."
)


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
            return ExtractionLLMResponse.model_validate(generated_json).to_domain()
        except ValidationError as exc:
            raise HFExtractionError("HF response failed schema validation") from exc

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
                {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
            "temperature": 0.0,
            "max_tokens": self._settings.hf_extraction_max_new_tokens,
        }


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
