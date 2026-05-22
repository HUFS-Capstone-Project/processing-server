from __future__ import annotations

from app.core.config import Settings
from app.services.crawler.extractors.types import (
    ExtractedContent,
    ExtractionMethod,
    SourceType,
)
from app.services.crawler.instagram_reel import is_instagram_host, is_instagram_media_url
from app.services.crawler.playwright_service import (
    fetch_instagram_media_result,
    instagram_fetch_metadata,
)


class InstagramContentExtractor:
    name = "instagram"

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def supports(self, url: str) -> bool:
        return is_instagram_media_url(url)

    def recognizes_host(self, url: str) -> bool:
        return is_instagram_host(url)

    async def extract(self, url: str) -> ExtractedContent:
        result = await fetch_instagram_media_result(url, self._settings)
        clean_text = (result.caption or "").strip()
        return ExtractedContent(
            source_url=url,
            source_type=SourceType.INSTAGRAM,
            content_text=clean_text,
            extraction_method=ExtractionMethod.INSTAGRAM_OG_META,
            raw_metadata=instagram_fetch_metadata(result),
            html=None,
        )
