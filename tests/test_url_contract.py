from __future__ import annotations

from app.domain.url_contract import (
    build_url_contract,
    canonical_url_for,
    crawl_url_for,
)


def test_url_contract_preserves_original_and_split_instagram_canonical_and_crawl_urls() -> None:
    original_url = "https://www.instagram.com/reels/DVDm96wjwWC/?igsh=abc"

    urls = build_url_contract(original_url)

    assert urls.original_url == original_url
    assert urls.canonical_url == "https://www.instagram.com/reel/DVDm96wjwWC/"
    assert urls.crawl_url == "https://www.instagram.com/reel/DVDm96wjwWC/"


def test_canonical_url_for_normalizes_generic_url_for_dedupe_only() -> None:
    url = "HTTPS://Example.com/Post/?b=2&a=1#fragment"

    assert canonical_url_for(url) == "https://example.com/Post?a=1&b=2"
    assert crawl_url_for(url) == url
