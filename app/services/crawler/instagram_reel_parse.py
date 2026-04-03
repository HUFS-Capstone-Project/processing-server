"""Instagram og 메타 한 덩어리 문자열 → 좋아요·댓글·유저·날짜·캡션 파싱."""

from __future__ import annotations

import re
from typing import TypedDict


class ParsedInstagramReelMeta(TypedDict):
    likes: int
    comments: int
    username: str
    posted_at: str
    caption: str


def _parse_count(s: str) -> int:
    s = (s or "").strip().replace(",", "").replace(" ", "")
    if not s:
        return 0
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([KkMmBb])?", s, re.IGNORECASE)
    if not m:
        return int(re.sub(r"[^\d]", "", s) or "0")
    num = float(m.group(1))
    suf = (m.group(2) or "").upper()
    mult = {"K": 1000, "M": 1_000_000, "B": 1_000_000_000}.get(suf, 1)
    return int(num * mult)


_COUNT = r"(?:\d+(?:\.\d+)?[KkMmBb]|\d{1,3}(?:,\d{3})+|\d+)"

_RE_QUOTED = re.compile(
    rf"^({_COUNT})\s+likes?,\s*({_COUNT})\s+comments?\s*-\s*(.+?)\s*-\s*([^:]+):\s*\"(.*)\"\s*\.?\s*$",
    re.DOTALL | re.IGNORECASE,
)

_RE_UNQUOTED = re.compile(
    rf"^({_COUNT})\s+likes?,\s*({_COUNT})\s+comments?\s*-\s*(.+?)\s*-\s*([^:]+):\s*(.+)\s*$",
    re.DOTALL | re.IGNORECASE,
)


def parse_instagram_reel_meta(text: str) -> ParsedInstagramReelMeta | None:
    raw = (text or "").strip()
    if not raw:
        return None

    m = _RE_QUOTED.match(raw)
    if m:
        likes_s, comments_s, username, posted_at, caption = m.groups()
        return {
            "likes": _parse_count(likes_s),
            "comments": _parse_count(comments_s),
            "username": username.strip(),
            "posted_at": posted_at.strip(),
            "caption": caption.strip(),
        }

    m = _RE_UNQUOTED.match(raw)
    if m:
        likes_s, comments_s, username, posted_at, caption = m.groups()
        return {
            "likes": _parse_count(likes_s),
            "comments": _parse_count(comments_s),
            "username": username.strip(),
            "posted_at": posted_at.strip(),
            "caption": caption.strip(),
        }

    return None
