from __future__ import annotations

from app.services.crawler.instagram_reel import (
    canonical_instagram_media_url,
    instagram_media_type,
    is_instagram_media_url,
    is_instagram_reel_url,
)


def test_instagram_reels_url_is_supported_as_reel_media() -> None:
    url = "https://www.instagram.com/reels/DVDm96wjwWC/"

    assert is_instagram_reel_url(url)
    assert is_instagram_media_url(url)
    assert instagram_media_type(url) == "reel"


def test_instagram_reels_url_canonicalizes_to_reel_shortcode_url() -> None:
    url = "https://www.instagram.com/reels/DVDm96wjwWC/?igsh=abc#fragment"

    assert canonical_instagram_media_url(url) == "https://www.instagram.com/reel/DVDm96wjwWC/"


def test_non_instagram_lookalike_host_is_not_supported() -> None:
    assert not is_instagram_media_url("https://notinstagram.com/reels/DVDm96wjwWC/")
