from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

YOUTUBE_VIDEO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")


def is_youtube_host(url: str) -> bool:
    try:
        host = (urlparse((url or "").strip()).netloc or "").lower()
    except Exception:
        return False
    return host == "youtu.be" or host == "youtube.com" or host.endswith(".youtube.com")


def extract_youtube_video_id(url: str) -> str | None:
    try:
        parsed = urlparse((url or "").strip())
    except Exception:
        return None

    host = (parsed.netloc or "").lower()
    parts = [part for part in (parsed.path or "").split("/") if part]

    if host == "youtu.be":
        return _valid_video_id(parts[0]) if len(parts) == 1 else None

    if host in {"youtube.com", "www.youtube.com"} and len(parts) == 2:
        if parts[0].lower() == "shorts":
            return _valid_video_id(parts[1])

    if host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        path = (parsed.path or "").rstrip("/")
        if path == "/watch":
            values = parse_qs(parsed.query).get("v") or []
            return _valid_video_id(values[0]) if len(values) == 1 else None

    return None


def canonical_youtube_video_url(url: str) -> str | None:
    video_id = extract_youtube_video_id(url)
    if not video_id:
        return None
    return f"https://www.youtube.com/watch?v={video_id}"


def _valid_video_id(value: str | None) -> str | None:
    video_id = (value or "").strip()
    return video_id if YOUTUBE_VIDEO_ID_PATTERN.fullmatch(video_id) else None
