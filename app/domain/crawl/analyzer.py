from __future__ import annotations

from app.core.config import Settings
from app.domain.job.model import CrawlArtifact
from app.services.crawler.instagram_reel import instagram_media_type, is_instagram_media_url
from app.services.crawler.instagram_reel_parse import parse_instagram_reel_meta
from app.services.crawler.playwright_service import fetch_page_content


async def crawl_and_parse(url: str, settings: Settings) -> CrawlArtifact:
    html, text = await fetch_page_content(url, settings)
    clean_text = (text or "").strip()

    media_type = instagram_media_type(url) if is_instagram_media_url(url) else None
    instagram_meta = None
    caption = clean_text or None

    if media_type and clean_text:
        parsed = parse_instagram_reel_meta(clean_text)
        if parsed:
            instagram_meta = parsed
            caption = parsed.get("caption") or clean_text

    return CrawlArtifact(
        url=url,
        html=html,
        text=clean_text,
        media_type=media_type,
        caption=caption,
        instagram_meta=instagram_meta,
    )
