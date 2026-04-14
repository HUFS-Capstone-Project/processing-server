"""Helpers to classify Instagram reel/post URLs."""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlparse


def _path_parts(url: str) -> list[str] | None:
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if "instagram.com" not in host:
            return None
        return [part for part in (parsed.path or "").split("/") if part]
    except Exception:
        return None


def is_instagram_reel_url(url: str) -> bool:
    parts = _path_parts(url)
    if not parts or len(parts) < 2:
        return False
    return parts[0].lower() == "reel" and bool(parts[1])


def is_instagram_post_url(url: str) -> bool:
    parts = _path_parts(url)
    if not parts or len(parts) < 2:
        return False
    return parts[0].lower() == "p" and bool(parts[1])


def is_instagram_media_url(url: str) -> bool:
    return is_instagram_reel_url(url) or is_instagram_post_url(url)


def instagram_media_type(url: str) -> Literal["reel", "post"] | None:
    if is_instagram_reel_url(url):
        return "reel"
    if is_instagram_post_url(url):
        return "post"
    return None
