from __future__ import annotations

import asyncio

import pytest

from app.core.config import Settings
from app.services.crawler.extractors.generic_web import GenericWebContentExtractor
from app.services.crawler.extractors.instagram import InstagramContentExtractor
from app.services.crawler.extractors.link_stats_registry import LinkStatsExtractorRegistry
from app.services.crawler.extractors.naver_blog import NaverBlogContentExtractor
from app.services.crawler.extractors.registry import ContentExtractorRegistry
from app.services.crawler.extractors.types import (
    ExtractedContent,
    ExtractionMethod,
    LinkStats,
    SourceType,
    StatsSource,
)
from app.services.crawler.extractors.instagram_link_stats import InstagramLinkStatsExtractor


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def test_registry_selects_instagram_extractor_for_instagram_media_url() -> None:
    registry = ContentExtractorRegistry(Settings())

    extractor = registry.select("https://www.instagram.com/reel/abc/")

    assert isinstance(extractor, InstagramContentExtractor)


def test_registry_selects_generic_extractor_for_generic_url() -> None:
    registry = ContentExtractorRegistry(Settings())

    extractor = registry.select("https://example.com/post")

    assert isinstance(extractor, GenericWebContentExtractor)


def test_registry_selects_naver_blog_extractor_for_naver_blog_url() -> None:
    registry = ContentExtractorRegistry(Settings())

    extractor = registry.select("https://blog.naver.com/example/123")

    assert isinstance(extractor, NaverBlogContentExtractor)


def test_naver_blog_extractor_returns_generic_fallback_content() -> None:
    class FallbackExtractor:
        name = "fake_generic"

        def supports(self, url: str) -> bool:
            return True

        async def extract(self, url: str) -> ExtractedContent:
            return ExtractedContent(
                source_url=url,
                source_type=SourceType.GENERIC_WEB,
                content_text="fallback body text",
                extraction_method=ExtractionMethod.GENERIC_WEB_INNER_TEXT,
                raw_metadata={"from": "fallback"},
                html="<html></html>",
            )

    extractor = NaverBlogContentExtractor(
        Settings(),
        fallback_extractor=FallbackExtractor(),
    )

    content = _run(extractor.extract("https://blog.naver.com/example/123"))

    assert content.source_type == SourceType.NAVER_BLOG
    assert content.extraction_method == ExtractionMethod.NAVER_BLOG_GENERIC_FALLBACK
    assert content.content_text == "fallback body text"
    assert content.raw_metadata == {
        "from": "fallback",
        "fallback_source_type": "GENERIC_WEB",
        "fallback_extraction_method": "GENERIC_WEB_INNER_TEXT",
    }


def test_link_stats_extractor_failure_returns_unavailable_stats() -> None:
    class FailingStatsExtractor:
        name = "failing_stats"

        def supports(self, url: str) -> bool:
            return True

        async def extract(self, content: ExtractedContent) -> LinkStats:
            raise RuntimeError("stats endpoint unavailable")

    registry = LinkStatsExtractorRegistry(
        extractors=[FailingStatsExtractor()],
        fallback_extractor=FailingStatsExtractor(),
    )
    content = ExtractedContent(
        source_url="https://example.com/post",
        source_type=SourceType.GENERIC_WEB,
        content_text="body",
    )

    stats = _run(registry.extract_best_effort(content))

    assert stats.stats_source == StatsSource.UNAVAILABLE
    assert stats.source_url == "https://example.com/post"
    assert stats.like_count is None
    assert "stats endpoint unavailable" in (stats.unavailable_reason or "")


def test_instagram_extractor_uses_public_playwright_wrapper(monkeypatch) -> None:
    calls: list[tuple[str, Settings]] = []

    async def fake_fetch(url: str, settings: Settings):
        calls.append((url, settings))
        return None, "instagram caption"

    monkeypatch.setattr(
        "app.services.crawler.extractors.instagram.fetch_instagram_media_content",
        fake_fetch,
    )
    settings = Settings()
    extractor = InstagramContentExtractor(settings)

    content = _run(extractor.extract("https://www.instagram.com/p/abc/"))

    assert calls == [("https://www.instagram.com/p/abc/", settings)]
    assert content.source_type == SourceType.INSTAGRAM
    assert content.extraction_method == ExtractionMethod.INSTAGRAM_OG_META
    assert content.content_text == "instagram caption"


def test_instagram_link_stats_extracts_existing_parser_fields() -> None:
    content = ExtractedContent(
        source_url="https://www.instagram.com/reel/abc/",
        source_type=SourceType.INSTAGRAM,
        content_text="caption",
        raw_metadata={
            "likes": 15000,
            "comments": 177,
            "posted_at": "April 2, 2026",
        },
    )

    stats = _run(InstagramLinkStatsExtractor().extract(content))

    assert stats.like_count == 15000
    assert stats.comment_count == 177
    assert stats.posted_at == "April 2, 2026"
    assert stats.stats_source == StatsSource.INSTAGRAM_META
