from __future__ import annotations

from urllib.parse import urlparse

from app.core.config import Settings
from app.services.crawler.extractors.base import ContentExtractor
from app.services.crawler.extractors.generic_web import GenericWebContentExtractor
from app.services.crawler.extractors.types import (
    ExtractedContent,
    ExtractionMethod,
    SourceType,
)


class NaverBlogContentExtractor:
    name = "naver_blog"

    def __init__(
        self,
        settings: Settings,
        *,
        fallback_extractor: ContentExtractor | None = None,
    ) -> None:
        self._settings = settings
        self._fallback_extractor = fallback_extractor or GenericWebContentExtractor(settings)

    def supports(self, url: str) -> bool:
        try:
            host = (urlparse(url).netloc or "").lower()
        except Exception:
            return False
        return host == "blog.naver.com" or host.endswith(".blog.naver.com")

    async def extract(self, url: str) -> ExtractedContent:
        fallback = await self._fallback_extractor.extract(url)
        raw_metadata = dict(fallback.raw_metadata or {})
        raw_metadata.update(
            {
                "fallback_source_type": fallback.source_type.value,
                "fallback_extraction_method": (
                    fallback.extraction_method.value if fallback.extraction_method else None
                ),
            }
        )
        # TODO: Implement Naver Blog-specific text extraction:
        # - Detect blog.naver.com/{blogId}/{logNo}
        # - Follow iframe/PostView when present
        # - Prefer se-main-container, se-component.se-text, se-module-text,
        #   and se-text-paragraph content containers
        # - Normalize zero-width spaces, blank lines, and duplicated whitespace
        # This layer must only collect stable text; place/address/phone pattern
        # extraction remains the LLM pipeline's responsibility.
        return ExtractedContent(
            source_url=url,
            source_type=SourceType.NAVER_BLOG,
            content_text=fallback.content_text,
            extraction_method=ExtractionMethod.NAVER_BLOG_GENERIC_FALLBACK,
            raw_metadata=raw_metadata,
            html=fallback.html,
        )
