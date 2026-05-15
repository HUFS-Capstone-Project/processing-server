from __future__ import annotations

from typing import Literal

from app.services.crawler.instagram_reel import is_instagram_media_url


def classify_source_url(source_url: str) -> Literal["instagram", "web"] | None:
    if is_instagram_media_url(source_url):
        return "instagram"
    return "web"

