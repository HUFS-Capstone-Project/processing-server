from __future__ import annotations

import logging

from app.services.crawler.extractors.generic_link_stats import GenericLinkStatsExtractor
from app.services.crawler.extractors.instagram_link_stats import InstagramLinkStatsExtractor
from app.services.crawler.extractors.link_stats_base import LinkStatsExtractor
from app.services.crawler.extractors.naver_blog_link_stats import NaverBlogLinkStatsExtractor
from app.services.crawler.extractors.youtube_link_stats import YouTubeLinkStatsExtractor
from app.services.crawler.extractors.types import (
    ExtractedContent,
    LinkStats,
    StatsConfidence,
    StatsSource,
)

logger = logging.getLogger("processing.crawler.link_stats")


class LinkStatsExtractorRegistry:
    def __init__(
        self,
        *,
        extractors: list[LinkStatsExtractor] | None = None,
        fallback_extractor: LinkStatsExtractor | None = None,
    ) -> None:
        fallback = fallback_extractor or GenericLinkStatsExtractor()
        self._fallback_extractor = fallback
        self._extractors = extractors or [
            InstagramLinkStatsExtractor(),
            NaverBlogLinkStatsExtractor(),
            YouTubeLinkStatsExtractor(),
            fallback,
        ]

    def select(self, url: str) -> LinkStatsExtractor:
        for extractor in self._extractors:
            if extractor.supports(url):
                return extractor
        return self._fallback_extractor

    async def extract_best_effort(self, content: ExtractedContent) -> LinkStats:
        extractor = self.select(content.source_url)
        try:
            return await extractor.extract(content)
        except Exception as exc:
            logger.warning(
                "link stats extraction failed source_url=%s selected_extractor=%s error=%s",
                content.source_url,
                extractor.name,
                exc.__class__.__name__,
                exc_info=True,
            )
            return LinkStats(
                source_url=content.source_url,
                source_type=content.source_type,
                stats_source=StatsSource.UNAVAILABLE,
                confidence=StatsConfidence.LOW,
                unavailable_reason=f"{exc.__class__.__name__}: {exc}",
                raw_stats={},
            )
