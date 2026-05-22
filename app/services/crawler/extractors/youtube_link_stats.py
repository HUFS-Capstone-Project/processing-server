from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.crawler.youtube import is_youtube_host
from app.services.crawler.extractors.types import (
    ExtractedContent,
    LinkStats,
    SourceType,
    StatsConfidence,
    StatsSource,
)


class YouTubeLinkStatsExtractor:
    name = "youtube_link_stats"

    def supports(self, url: str) -> bool:
        return is_youtube_host(url)

    async def extract(self, content: ExtractedContent) -> LinkStats:
        raw = content.raw_metadata or {}
        youtube = raw.get("youtube") if isinstance(raw.get("youtube"), dict) else {}
        statistics = youtube.get("statistics") if isinstance(youtube.get("statistics"), dict) else {}
        snippet = youtube.get("snippet") if isinstance(youtube.get("snippet"), dict) else {}

        like_count = _optional_int(statistics.get("likeCount"))
        comment_count = _optional_int(statistics.get("commentCount"))
        posted_at = _optional_str(snippet.get("publishedAt"))
        raw_stats = {
            "like_count": like_count,
            "comment_count": comment_count,
            "posted_at": posted_at,
        }

        if like_count is None and comment_count is None and posted_at is None:
            return LinkStats(
                source_url=content.source_url,
                source_type=SourceType.YOUTUBE,
                collected_at=datetime.now(timezone.utc),
                stats_source=StatsSource.UNAVAILABLE,
                confidence=StatsConfidence.LOW,
                unavailable_reason="YouTube metadata did not include link stats.",
                raw_stats=raw_stats,
            )

        return LinkStats(
            source_url=content.source_url,
            source_type=SourceType.YOUTUBE,
            like_count=like_count,
            comment_count=comment_count,
            posted_at=posted_at,
            collected_at=datetime.now(timezone.utc),
            stats_source=StatsSource.YOUTUBE_DATA_API,
            confidence=StatsConfidence.HIGH,
            raw_stats=raw_stats,
        )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
