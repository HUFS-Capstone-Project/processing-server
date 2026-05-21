from __future__ import annotations

import asyncio

import pytest

from app.core.config import Settings
from app.domain.crawl.analyzer import crawl_and_parse
from app.services.crawler.extractors.types import (
    ExtractedContent,
    ExtractionMethod,
    SourceType,
)


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        coro.close()
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


class FakeInstagramExtractor:
    name = "instagram"


class FakeContentExtractorRegistry:
    calls: list[str] = []

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def extract(self, url: str):
        self.__class__.calls.append(url)
        content_text = (
            '2,301 likes, 6 comments - bbang.morning - April 18, 2026: '
            '"Mango dessert cafe"'
        )
        return FakeInstagramExtractor(), ExtractedContent(
            source_url=url,
            source_type=SourceType.INSTAGRAM,
            content_text=content_text,
            extraction_method=ExtractionMethod.INSTAGRAM_OG_META,
        )


def test_instagram_crawl_artifact_uses_caption_only_content_text(monkeypatch) -> None:
    FakeContentExtractorRegistry.calls = []
    monkeypatch.setattr(
        "app.domain.crawl.analyzer.ContentExtractorRegistry",
        FakeContentExtractorRegistry,
    )

    artifact = _run(crawl_and_parse("https://www.instagram.com/reel/example/", Settings()))

    assert artifact.content_text == "Mango dessert cafe"
    assert artifact.raw_metadata == {}
    assert artifact.link_stats is not None
    assert artifact.link_stats.like_count == 2301
    assert artifact.link_stats.comment_count == 6
    assert artifact.link_stats.posted_at == "April 18, 2026"


def test_instagram_reels_crawl_uses_canonical_reel_url(monkeypatch) -> None:
    FakeContentExtractorRegistry.calls = []
    monkeypatch.setattr(
        "app.domain.crawl.analyzer.ContentExtractorRegistry",
        FakeContentExtractorRegistry,
    )

    artifact = _run(crawl_and_parse("https://www.instagram.com/reels/example/?igsh=abc", Settings()))

    assert FakeContentExtractorRegistry.calls == ["https://www.instagram.com/reel/example/"]
    assert artifact.url == "https://www.instagram.com/reel/example/"
    assert artifact.media_type == "reel"
