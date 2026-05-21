from __future__ import annotations

from app.domain.url_contract import (
    build_url_contract,
    canonical_naver_blog_url,
    canonical_url_for,
    crawl_url_for,
)

NAVER_BLOG_CANONICAL = "https://blog.naver.com/fkawnldhkd/224279607194"


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


def test_canonical_naver_blog_url_normalizes_desktop_path_url() -> None:
    url = "https://blog.naver.com/fkawnldhkd/224279607194"

    assert canonical_naver_blog_url(url) == NAVER_BLOG_CANONICAL
    assert canonical_url_for(url) == NAVER_BLOG_CANONICAL


def test_canonical_naver_blog_url_normalizes_mobile_path_url() -> None:
    url = "https://m.blog.naver.com/fkawnldhkd/224279607194"

    assert canonical_naver_blog_url(url) == NAVER_BLOG_CANONICAL
    assert canonical_url_for(url) == NAVER_BLOG_CANONICAL


def test_canonical_naver_blog_url_strips_query_and_fragment() -> None:
    url = "https://blog.naver.com/fkawnldhkd/224279607194?trackingCode=blog&utm_source=share#section"

    assert canonical_naver_blog_url(url) == NAVER_BLOG_CANONICAL
    assert canonical_url_for(url) == NAVER_BLOG_CANONICAL


def test_canonical_naver_blog_url_falls_back_to_generic_for_non_naver_blog_urls() -> None:
    url = "HTTPS://Example.com/Post/?b=2&a=1#fragment"

    assert canonical_naver_blog_url(url) is None
    assert canonical_url_for(url) == "https://example.com/Post?a=1&b=2"


def test_canonical_naver_blog_url_rejects_non_numeric_log_no() -> None:
    url = "https://blog.naver.com/fkawnldhkd/not-a-number"

    assert canonical_naver_blog_url(url) is None
    assert canonical_url_for(url) == "https://blog.naver.com/fkawnldhkd/not-a-number"
