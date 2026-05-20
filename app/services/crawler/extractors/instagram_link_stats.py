from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.crawler.extractors.types import (
    ExtractedContent,
    LinkStats,
    SourceType,
    StatsConfidence,
    StatsSource,
)
from app.services.crawler.instagram_reel import is_instagram_media_url


class InstagramLinkStatsExtractor:
    name = "instagram_link_stats"

    def supports(self, url: str) -> bool:
        return is_instagram_media_url(url)

    async def extract(self, content: ExtractedContent) -> LinkStats:
        raw = content.raw_metadata or {}
        like_count = _optional_int(raw.get("like_count", raw.get("likes")))
        comment_count = _optional_int(raw.get("comment_count", raw.get("comments")))
        posted_at = _optional_str(raw.get("posted_at"))
        raw_stats = {
            "like_count_text": _optional_str(raw.get("like_count_text", raw.get("likes_text"))),
            "comment_count_text": _optional_str(
                raw.get("comment_count_text", raw.get("comments_text"))
            ),
            "posted_at_text": _optional_str(raw.get("posted_at_text", raw.get("posted_at"))),
        }
        if like_count is None and comment_count is None and posted_at is None:
            return LinkStats(
                source_url=content.source_url,
                source_type=SourceType.INSTAGRAM,
                collected_at=datetime.now(timezone.utc),
                stats_source=StatsSource.UNAVAILABLE,
                confidence=StatsConfidence.LOW,
                unavailable_reason="Instagram metadata did not include link stats.",
                raw_stats=raw_stats,
            )

        return LinkStats(
            source_url=content.source_url,
            source_type=SourceType.INSTAGRAM,
            like_count=like_count,
            comment_count=comment_count,
            posted_at=posted_at,
            collected_at=datetime.now(timezone.utc),
            stats_source=StatsSource.INSTAGRAM_META,
            confidence=StatsConfidence.MEDIUM,
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
