from __future__ import annotations

import asyncio
import os

import pytest

from app.core.config import Settings
from app.domain.job import extracted_places_from_result
from app.infra.llm import HFExtractionClient, HFOCRClient
from app.services.crawler.playwright_service import (
    fetch_instagram_media_result,
    fetch_instagram_post_images,
)
from app.worker.processor import build_instagram_ocr_augmented_content


LIVE_POST_URLS = [
    "https://www.instagram.com/p/DLmXSK3znl-",
    "https://www.instagram.com/p/C6Kp79xRiGC",
    "https://www.instagram.com/p/DYgvDx0gc_X",
    "https://www.instagram.com/p/DX8pE6WEeom",
]


def _run_with_subprocesses(coro):
    if hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        return asyncio.run(coro)
    finally:
        if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def _run_live_ocr_fallback_pipeline(url: str, settings: Settings):
    caption_result = await fetch_instagram_media_result(url, settings)
    image_result = await fetch_instagram_post_images(url, settings)
    ocr_texts = await HFOCRClient(settings).extract_texts_from_image_urls(image_result.image_urls)
    augmented = build_instagram_ocr_augmented_content(
        caption=caption_result.caption,
        ocr_texts=ocr_texts,
    )
    extraction = await HFExtractionClient(settings).extract(
        text=augmented,
        original_url=url,
        media_type="post",
    )
    return image_result, ocr_texts, augmented, extraction


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_INSTAGRAM_OCR_FALLBACK_TESTS") != "1",
    reason="Set RUN_LIVE_INSTAGRAM_OCR_FALLBACK_TESTS=1 to call live Instagram and HF APIs.",
)
@pytest.mark.parametrize("url", LIVE_POST_URLS)
def test_live_instagram_ocr_fallback_pipeline_extracts_places(url: str) -> None:
    settings = Settings(
        hf_ocr_timeout_seconds=60,
        hf_ocr_max_attempts=1,
        hf_ocr_max_new_tokens=1024,
        hf_extraction_timeout_seconds=60,
        hf_extraction_max_new_tokens=2048,
        instagram_image_fetch_max_images=10,
        instagram_image_fetch_timeout_ms=12000,
        crawler_browser_reuse_enabled=False,
    )
    if not (settings.hf_extraction_endpoint_url and settings.hf_extraction_api_token):
        pytest.skip("HF extraction endpoint/token are not configured")

    image_result, ocr_texts, augmented, extraction = _run_with_subprocesses(
        _run_live_ocr_fallback_pipeline(url, settings)
    )

    assert image_result.response_status == 200
    assert len(image_result.image_urls) >= 1
    assert ocr_texts
    assert "[image_ocr]" in augmented
    assert extraction is not None
    assert extracted_places_from_result(extraction)
