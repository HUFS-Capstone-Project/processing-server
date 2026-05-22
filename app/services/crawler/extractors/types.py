from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class SourceType(str, Enum):
    INSTAGRAM = "INSTAGRAM"
    NAVER_BLOG = "NAVER_BLOG"
    YOUTUBE = "YOUTUBE"
    GENERIC_WEB = "GENERIC_WEB"


class ExtractionMethod(str, Enum):
    INSTAGRAM_OG_META = "INSTAGRAM_OG_META"
    GENERIC_WEB_INNER_TEXT = "GENERIC_WEB_INNER_TEXT"
    NAVER_BLOG_POST_VIEW = "NAVER_BLOG_POST_VIEW"
    NAVER_BLOG_GENERIC_FALLBACK = "NAVER_BLOG_GENERIC_FALLBACK"
    YOUTUBE_DATA_API = "YOUTUBE_DATA_API"


class StatsSource(str, Enum):
    META_TAG = "META_TAG"
    INSTAGRAM_META = "INSTAGRAM_META"
    NAVER_BLOG_DOM = "NAVER_BLOG_DOM"
    YOUTUBE_DATA_API = "YOUTUBE_DATA_API"
    SCRAPED = "SCRAPED"
    UNAVAILABLE = "UNAVAILABLE"


class StatsConfidence(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass(slots=True)
class LinkStats:
    source_url: str
    source_type: SourceType
    like_count: int | None = None
    comment_count: int | None = None
    posted_at: str | None = None
    collected_at: datetime | None = None
    stats_source: StatsSource = StatsSource.UNAVAILABLE
    confidence: StatsConfidence = StatsConfidence.LOW
    unavailable_reason: str | None = None
    raw_stats: dict[str, Any] | None = None

    @property
    def is_available(self) -> bool:
        return self.stats_source != StatsSource.UNAVAILABLE and any(
            value is not None
            for value in (
                self.like_count,
                self.comment_count,
                self.posted_at,
            )
        )


@dataclass(slots=True)
class ExtractedContent:
    source_url: str
    source_type: SourceType
    content_text: str
    extraction_method: ExtractionMethod | None = None
    raw_metadata: dict[str, Any] | None = None
    html: str | None = None
