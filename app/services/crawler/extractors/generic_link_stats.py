from __future__ import annotations

from app.services.crawler.extractors.types import (
    ExtractedContent,
    LinkStats,
    StatsConfidence,
    StatsSource,
)


class GenericLinkStatsExtractor:
    name = "generic_link_stats"

    def supports(self, url: str) -> bool:
        _ = url
        return True

    async def extract(self, content: ExtractedContent) -> LinkStats:
        return LinkStats(
            source_url=content.source_url,
            source_type=content.source_type,
            stats_source=StatsSource.UNAVAILABLE,
            confidence=StatsConfidence.LOW,
            unavailable_reason="No link stats extractor is implemented for this source.",
            raw_stats={},
        )
