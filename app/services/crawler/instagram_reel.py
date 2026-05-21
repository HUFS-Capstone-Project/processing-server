"""Compatibility imports for Instagram media URL helpers."""

from __future__ import annotations

from app.domain.url_contract import (
    canonical_instagram_media_url,
    instagram_media_type,
    is_instagram_media_url,
    is_instagram_post_url,
    is_instagram_reel_url,
)

__all__ = [
    "canonical_instagram_media_url",
    "instagram_media_type",
    "is_instagram_media_url",
    "is_instagram_post_url",
    "is_instagram_reel_url",
]
