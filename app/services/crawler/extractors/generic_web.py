from __future__ import annotations

from app.core.config import Settings
from app.services.crawler.extractors.types import (
    ExtractedContent,
    ExtractionMethod,
    SourceType,
)
from app.services.crawler.playwright_service import fetch_generic_web_content


class GenericWebContentExtractor:
    name = "generic_web"

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def supports(self, url: str) -> bool:
        _ = url
        return True

    async def extract(self, url: str) -> ExtractedContent:
        html, text = await fetch_generic_web_content(url, self._settings)
        clean_text = (text or "").strip()
        # TODO: Add safe article/main/role=main/meta fallbacks without changing
        # current generic web innerText behavior.
        return ExtractedContent(
            source_url=url,
            source_type=SourceType.GENERIC_WEB,
            content_text=clean_text,
            extraction_method=ExtractionMethod.GENERIC_WEB_INNER_TEXT,
            raw_metadata={},
            html=html,
        )
