from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit

from app.services.crawler.youtube import canonical_youtube_video_url


@dataclass(frozen=True, slots=True)
class SourceUrls:
    original_url: str
    canonical_url: str
    crawl_url: str


def build_url_contract(original_url: str) -> SourceUrls:
    original = (original_url or "").strip()
    canonical = canonical_url_for(original)
    return SourceUrls(
        original_url=original,
        canonical_url=canonical,
        crawl_url=crawl_url_for(original),
    )


def canonical_url_for(original_url: str) -> str:
    instagram_url = canonical_instagram_media_url(original_url)
    if instagram_url:
        return instagram_url
    youtube_url = canonical_youtube_video_url(original_url)
    if youtube_url:
        return youtube_url
    naver_blog_url = canonical_naver_blog_url(original_url)
    if naver_blog_url:
        return naver_blog_url
    return canonical_generic_url(original_url)


def crawl_url_for(original_url: str) -> str:
    return (
        canonical_instagram_media_url(original_url)
        or canonical_youtube_video_url(original_url)
        or (original_url or "").strip()
    )


def canonical_generic_url(original_url: str) -> str:
    parsed = urlsplit((original_url or "").strip())
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, query, ""))


def instagram_media_type(url: str) -> Literal["reel", "post"] | None:
    media = _instagram_media_parts(url)
    return media[0] if media else None


def is_instagram_media_url(url: str) -> bool:
    return _instagram_media_parts(url) is not None


def is_instagram_host(url: str) -> bool:
    return _instagram_path_parts(url) is not None


def is_instagram_reel_url(url: str) -> bool:
    media = _instagram_media_parts(url)
    return media is not None and media[0] == "reel"


def is_instagram_post_url(url: str) -> bool:
    media = _instagram_media_parts(url)
    return media is not None and media[0] == "post"


def canonical_naver_blog_url(url: str) -> str | None:
    try:
        parsed = urlparse((url or "").strip())
        host = (parsed.netloc or "").lower()
        if host not in {"blog.naver.com", "m.blog.naver.com"}:
            return None

        parts = [part for part in (parsed.path or "").split("/") if part]
        if len(parts) != 2:
            return None

        blog_id, log_no = parts[0].strip(), parts[1].strip()
        if not blog_id or not log_no.isdigit():
            return None

        return f"https://blog.naver.com/{blog_id}/{log_no}"
    except Exception:
        return None


def canonical_instagram_media_url(url: str) -> str | None:
    media = _instagram_media_parts(url)
    if not media:
        return None

    media_type, shortcode = media
    path_type = "reel" if media_type == "reel" else "p"
    return f"https://www.instagram.com/{path_type}/{shortcode}/"


def _instagram_path_parts(url: str) -> list[str] | None:
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if host != "instagram.com" and not host.endswith(".instagram.com"):
            return None
        return [part for part in (parsed.path or "").split("/") if part]
    except Exception:
        return None


def _instagram_media_parts(url: str) -> tuple[Literal["reel", "post"], str] | None:
    parts = _instagram_path_parts(url)
    if not parts or len(parts) < 2:
        return None

    media_path = parts[0].lower()
    shortcode = parts[1].strip()
    if not shortcode:
        return None
    if media_path in {"reel", "reels"}:
        return "reel", shortcode
    if media_path == "p":
        return "post", shortcode
    return None
