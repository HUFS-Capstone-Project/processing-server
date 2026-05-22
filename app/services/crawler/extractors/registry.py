from __future__ import annotations

from app.core.config import Settings
from app.services.crawler.extractors.base import ContentExtractor
from app.services.crawler.extractors.generic_web import GenericWebContentExtractor
from app.services.crawler.extractors.instagram import InstagramContentExtractor
from app.services.crawler.extractors.naver_blog import NaverBlogContentExtractor
from app.services.crawler.extractors.types import ExtractedContent
from app.services.crawler.extractors.youtube import YouTubeContentExtractor


class ContentExtractorRegistry:
    def __init__(
        self,
        settings: Settings,
        *,
        extractors: list[ContentExtractor] | None = None,
        fallback_extractor: ContentExtractor | None = None,
    ) -> None:
        generic = fallback_extractor or GenericWebContentExtractor(settings)
        self._fallback_extractor = generic
        self._extractors = extractors or [
            InstagramContentExtractor(settings),
            NaverBlogContentExtractor(settings, fallback_extractor=generic),
            YouTubeContentExtractor(settings),
            generic,
        ]

    def select(self, url: str) -> ContentExtractor:
        for extractor in self._extractors:
            if extractor.supports(url):
                return extractor
        return self._fallback_extractor

    async def extract(self, url: str) -> tuple[ContentExtractor, ExtractedContent]:
        extractor = self.select(url)
        return extractor, await extractor.extract(url)
