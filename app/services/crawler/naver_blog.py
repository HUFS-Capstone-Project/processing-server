from __future__ import annotations

from urllib.parse import urlparse

from app.domain.url_contract import canonical_naver_blog_url


def is_naver_blog_host(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    return host in {"blog.naver.com", "m.blog.naver.com"}


def is_naver_blog_url(url: str) -> bool:
    return canonical_naver_blog_url(url) is not None
