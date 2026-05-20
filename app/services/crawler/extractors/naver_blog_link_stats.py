from __future__ import annotations

import re
from datetime import datetime, timezone

from app.services.crawler.naver_blog import is_naver_blog_url
from app.services.crawler.extractors.types import (
    ExtractedContent,
    LinkStats,
    SourceType,
    StatsConfidence,
    StatsSource,
)


class NaverBlogLinkStatsExtractor:
    name = "naver_blog_link_stats"

    def supports(self, url: str) -> bool:
        return is_naver_blog_url(url)

    async def extract(self, content: ExtractedContent) -> LinkStats:
        raw = content.raw_metadata or {}
        naver_blog = raw.get("naver_blog") if isinstance(raw.get("naver_blog"), dict) else {}
        like_count_text = str(
            naver_blog.get("like_count_text") or raw.get("naver_blog_like_count_text") or ""
        ).strip()
        comment_count_text = str(
            naver_blog.get("comment_count_text") or raw.get("naver_blog_comment_count_text") or ""
        ).strip()
        posted_at_text = str(
            naver_blog.get("posted_at_text") or raw.get("naver_blog_posted_at_text") or ""
        ).strip()
        like_count = _parse_like_count(like_count_text)
        comment_count = _parse_count(comment_count_text)
        posted_at = posted_at_text or None
        raw_stats = {
            "like_count_text": like_count_text or None,
            "comment_count_text": comment_count_text or None,
            "posted_at_text": posted_at_text or None,
        }
        if like_count is None and comment_count is None and posted_at is None:
            return LinkStats(
                source_url=content.source_url,
                source_type=SourceType.NAVER_BLOG,
                collected_at=datetime.now(timezone.utc),
                stats_source=StatsSource.UNAVAILABLE,
                confidence=StatsConfidence.LOW,
                unavailable_reason="Naver Blog metadata did not include parseable DOM stats.",
                raw_stats=raw_stats,
            )

        return LinkStats(
            source_url=content.source_url,
            source_type=SourceType.NAVER_BLOG,
            like_count=like_count,
            comment_count=comment_count,
            posted_at=posted_at,
            collected_at=datetime.now(timezone.utc),
            stats_source=StatsSource.NAVER_BLOG_DOM,
            confidence=StatsConfidence.MEDIUM,
            raw_stats=raw_stats,
        )


def _parse_like_count(text: str) -> int | None:
    return _parse_count(text)


def _parse_count(text: str) -> int | None:
    digits = "".join(re.findall(r"\d+", text or ""))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None
