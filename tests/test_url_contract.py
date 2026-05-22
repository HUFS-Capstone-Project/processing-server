from __future__ import annotations

from app.domain.url_contract import (
    build_url_contract,
    canonical_naver_blog_url,
    canonical_url_for,
    crawl_url_for,
)
from app.services.crawler.youtube import (
    canonical_youtube_video_url,
    extract_youtube_video_id,
    is_youtube_host,
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


def test_youtube_url_contract_normalizes_supported_video_urls() -> None:
    cases = [
        (
            "https://youtube.com/shorts/fxQn26cv8KE?si=HCQ7t7zwzznUwtEj",
            "fxQn26cv8KE",
        ),
        (
            "https://www.youtube.com/shorts/fxQn26cv8KE?si=HCQ7t7zwzznUwtEj",
            "fxQn26cv8KE",
        ),
        (
            "https://youtu.be/ZJMi3m8spJA?si=YG0pDP1ABFUunMvl",
            "ZJMi3m8spJA",
        ),
        (
            "https://www.youtube.com/watch?v=fxQn26cv8KE&si=abc",
            "fxQn26cv8KE",
        ),
        (
            "https://youtube.com/watch?v=fxQn26cv8KE",
            "fxQn26cv8KE",
        ),
        (
            "https://m.youtube.com/watch?v=fxQn26cv8KE",
            "fxQn26cv8KE",
        ),
    ]

    for url, video_id in cases:
        canonical = f"https://www.youtube.com/watch?v={video_id}"
        assert is_youtube_host(url) is True
        assert extract_youtube_video_id(url) == video_id
        assert canonical_youtube_video_url(url) == canonical
        assert canonical_url_for(url) == canonical
        assert crawl_url_for(url) == canonical


def test_youtube_host_urls_without_supported_video_id_are_not_canonicalized() -> None:
    cases = [
        "https://www.youtube.com/@channel",
        "https://www.youtube.com/playlist?list=abc",
        "https://www.youtube.com/results?search_query=cafe",
        "https://youtu.be/not-valid",
    ]

    for url in cases:
        assert is_youtube_host(url) is True
        assert extract_youtube_video_id(url) is None
        assert canonical_youtube_video_url(url) is None
