from __future__ import annotations

from app.core.config import Settings
from app.services.crawler.extractors.types import (
    ExtractedContent,
    ExtractionMethod,
    SourceType,
)
from app.services.crawler.instagram_reel import is_instagram_host, is_instagram_media_url
from app.services.crawler.instagram_http_meta import (
    fetch_instagram_http_meta,
    instagram_http_meta_metadata,
)
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
        http_meta = await fetch_instagram_http_meta(url, self._settings)
        if http_meta.selected.useful or http_meta.selected.rate_limited or http_meta.selected.challenge:
            result = http_meta.selected
            return ExtractedContent(
                source_url=url,
                source_type=SourceType.INSTAGRAM,
                content_text=result.cleaned_description if result.useful else "",
                extraction_method=ExtractionMethod.INSTAGRAM_OG_META,
                raw_metadata=instagram_http_meta_metadata(result),
                html=None,
            )

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
