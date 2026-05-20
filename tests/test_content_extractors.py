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
    StatsConfidence,
    StatsSource,
)
from app.services.crawler.extractors.instagram_link_stats import InstagramLinkStatsExtractor
from app.services.crawler.extractors.naver_blog_link_stats import NaverBlogLinkStatsExtractor
from app.services.crawler.playwright_service import NaverBlogFetchResult


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        coro.close()
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


def test_naver_blog_extractor_returns_dedicated_content(monkeypatch) -> None:
    async def fake_fetch(url: str, _settings: Settings):
        return NaverBlogFetchResult(
            html="<html>raw</html>",
            content_text="naver body text",
            resolved_url="https://blog.naver.com/PostView.naver?blogId=example&logNo=123",
            extraction_source="iframe_post_view",
            selected_selector="#post-view123 > div > div.se-main-container",
            iframe_src="/PostView.naver?blogId=example&logNo=123",
            log_no="123",
            like_count_text="4",
            comment_count_text="11",
            posted_at_text="2026. 2. 5. 2:08",
        )

    monkeypatch.setattr(
        "app.services.crawler.extractors.naver_blog.fetch_naver_blog_content",
        fake_fetch,
    )
    extractor = NaverBlogContentExtractor(Settings())

    content = _run(extractor.extract("https://blog.naver.com/example/123"))

    assert content.source_type == SourceType.NAVER_BLOG
    assert content.extraction_method == ExtractionMethod.NAVER_BLOG_POST_VIEW
    assert content.content_text == "naver body text"
    assert content.html == "<html>raw</html>"
    assert content.raw_metadata == {
        "extraction_source": "iframe_post_view",
        "final_url": "https://blog.naver.com/PostView.naver?blogId=example&logNo=123",
        "html_len": 16,
        "body_text_len": 15,
        "empty_body": False,
        "naver_blog": {
            "extraction_source": "iframe_post_view",
            "resolved_url": "https://blog.naver.com/PostView.naver?blogId=example&logNo=123",
            "iframe_src": "/PostView.naver?blogId=example&logNo=123",
            "log_no": "123",
            "selected_selector": "#post-view123 > div > div.se-main-container",
            "like_count_text": "4",
            "comment_count_text": "11",
            "posted_at_text": "2026. 2. 5. 2:08",
        },
    }
    assert "html" not in content.raw_metadata


def test_naver_blog_extractor_keeps_post_view_method_for_direct_selector(monkeypatch) -> None:
    async def fake_fetch(url: str, _settings: Settings):
        return NaverBlogFetchResult(
            html=None,
            content_text="direct body",
            resolved_url=url,
            extraction_source="direct_selector",
            selected_selector=".se-main-container",
            log_no="123",
        )

    monkeypatch.setattr(
        "app.services.crawler.extractors.naver_blog.fetch_naver_blog_content",
        fake_fetch,
    )
    extractor = NaverBlogContentExtractor(Settings())

    content = _run(extractor.extract("https://blog.naver.com/example/123"))

    assert content.source_type == SourceType.NAVER_BLOG
    assert content.extraction_method == ExtractionMethod.NAVER_BLOG_POST_VIEW
    assert content.raw_metadata["naver_blog"]["extraction_source"] == "direct_selector"


def test_naver_blog_extractor_returns_generic_fallback_content(monkeypatch) -> None:
    async def fake_fetch(url: str, _settings: Settings):
        raise RuntimeError("dedicated crawler unavailable")

    monkeypatch.setattr(
        "app.services.crawler.extractors.naver_blog.fetch_naver_blog_content",
        fake_fetch,
    )

    class FallbackExtractor:
        name = "fake_generic"

        def __init__(self) -> None:
            self.calls: list[str] = []

        def supports(self, url: str) -> bool:
            return True

        async def extract(self, url: str) -> ExtractedContent:
            self.calls.append(url)
            return ExtractedContent(
                source_url=url,
                source_type=SourceType.GENERIC_WEB,
                content_text="fallback body text",
                extraction_method=ExtractionMethod.GENERIC_WEB_INNER_TEXT,
                raw_metadata={"from": "fallback"},
                html="<html></html>",
            )

    fallback = FallbackExtractor()
    extractor = NaverBlogContentExtractor(
        Settings(),
        fallback_extractor=fallback,
    )

    content = _run(extractor.extract("https://blog.naver.com/example/123"))

    assert fallback.calls == ["https://blog.naver.com/example/123"]
    assert content.source_type == SourceType.NAVER_BLOG
    assert content.extraction_method == ExtractionMethod.NAVER_BLOG_GENERIC_FALLBACK
    assert content.content_text == "fallback body text"
    assert content.raw_metadata == {
        "from": "fallback",
        "extraction_source": "generic_fallback",
        "final_url": "https://blog.naver.com/example/123",
        "html_len": 13,
        "body_text_len": 18,
        "empty_body": False,
        "naver_blog": {
            "extraction_source": "generic_fallback",
            "resolved_url": "https://blog.naver.com/example/123",
            "iframe_src": None,
            "log_no": None,
            "selected_selector": None,
            "like_count_text": None,
            "comment_count_text": None,
            "posted_at_text": None,
        },
        "fallback_source_type": "GENERIC_WEB",
        "fallback_extraction_method": "GENERIC_WEB_INNER_TEXT",
    }


def test_naver_blog_extractor_falls_back_when_dedicated_content_empty(monkeypatch) -> None:
    async def fake_fetch(url: str, _settings: Settings):
        return NaverBlogFetchResult(
            html="<html></html>",
            content_text="",
            resolved_url=url,
            extraction_source="direct_selector",
        )

    monkeypatch.setattr(
        "app.services.crawler.extractors.naver_blog.fetch_naver_blog_content",
        fake_fetch,
    )

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
                raw_metadata={},
                html="<html>fallback</html>",
            )

    extractor = NaverBlogContentExtractor(Settings(), fallback_extractor=FallbackExtractor())

    content = _run(extractor.extract("https://blog.naver.com/example/123"))

    assert content.source_type == SourceType.NAVER_BLOG
    assert content.extraction_method == ExtractionMethod.NAVER_BLOG_GENERIC_FALLBACK
    assert content.content_text == "fallback body text"


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
        from app.services.crawler.playwright_service import InstagramFetchResult

        return InstagramFetchResult(
            caption="instagram caption",
            og_source="og:description",
            og_wait_timed_out=False,
            early_extract_hit=True,
            blocked_resource_count=2,
            launch_ms=0,
            context_ms=1,
            goto_ms=2,
            og_wait_ms=0,
            extract_ms=1,
            total_ms=4,
            response_status=200,
            final_url="https://www.instagram.com/p/abc/?token=redacted",
            html_len=123,
            body_text_len=10,
            og_meta_count=3,
            og_description_present=True,
            og_title_present=True,
        )

    monkeypatch.setattr(
        "app.services.crawler.extractors.instagram.fetch_instagram_media_result",
        fake_fetch,
    )
    settings = Settings()
    extractor = InstagramContentExtractor(settings)

    content = _run(extractor.extract("https://www.instagram.com/p/abc/"))

    assert calls == [("https://www.instagram.com/p/abc/", settings)]
    assert content.source_type == SourceType.INSTAGRAM
    assert content.extraction_method == ExtractionMethod.INSTAGRAM_OG_META
    assert content.content_text == "instagram caption"
    assert content.raw_metadata["instagram"]["og_source"] == "og:description"
    assert content.raw_metadata["response_status"] == 200
    assert content.raw_metadata["final_url"] == "https://www.instagram.com/p/abc/"
    assert "caption" not in content.raw_metadata


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
    assert stats.raw_stats == {
        "like_count_text": None,
        "comment_count_text": None,
        "posted_at_text": "April 2, 2026",
    }


def test_naver_blog_link_stats_extracts_like_count_from_metadata() -> None:
    content = ExtractedContent(
        source_url="https://blog.naver.com/example/123",
        source_type=SourceType.NAVER_BLOG,
        content_text="body",
        raw_metadata={"naver_blog": {"like_count_text": "좋아요 1,234"}},
    )

    stats = _run(NaverBlogLinkStatsExtractor().extract(content))

    assert stats.source_type == SourceType.NAVER_BLOG
    assert stats.like_count == 1234
    assert stats.comment_count is None
    assert stats.stats_source == StatsSource.NAVER_BLOG_DOM
    assert stats.confidence == StatsConfidence.MEDIUM
    assert stats.raw_stats == {
        "like_count_text": "좋아요 1,234",
        "comment_count_text": None,
        "posted_at_text": None,
    }


def test_naver_blog_link_stats_extracts_comment_count_and_posted_at_from_metadata() -> None:
    content = ExtractedContent(
        source_url="https://blog.naver.com/example/123",
        source_type=SourceType.NAVER_BLOG,
        content_text="body",
        raw_metadata={"naver_blog": {"comment_count_text": "11", "posted_at_text": "2026. 2. 5. 2:08"}},
    )

    stats = _run(NaverBlogLinkStatsExtractor().extract(content))

    assert stats.source_type == SourceType.NAVER_BLOG
    assert stats.like_count is None
    assert stats.comment_count == 11
    assert stats.posted_at == "2026. 2. 5. 2:08"
    assert stats.stats_source == StatsSource.NAVER_BLOG_DOM
    assert stats.confidence == StatsConfidence.MEDIUM
    assert stats.raw_stats == {
        "like_count_text": None,
        "comment_count_text": "11",
        "posted_at_text": "2026. 2. 5. 2:08",
    }


def test_naver_blog_link_stats_returns_unavailable_without_parseable_like_count() -> None:
    content = ExtractedContent(
        source_url="https://blog.naver.com/example/123",
        source_type=SourceType.NAVER_BLOG,
        content_text="body",
        raw_metadata={},
    )

    stats = _run(NaverBlogLinkStatsExtractor().extract(content))

    assert stats.like_count is None
    assert stats.stats_source == StatsSource.UNAVAILABLE
    assert stats.confidence == StatsConfidence.LOW
