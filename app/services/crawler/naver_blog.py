from __future__ import annotations

from urllib.parse import urlparse


def is_naver_blog_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    return host == "blog.naver.com" or host.endswith(".blog.naver.com")
