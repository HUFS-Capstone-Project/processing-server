"""크롤 → (Instagram이면) 파싱 → `AnalyzeResponse` 조립."""

from app.core.config import get_settings
from app.schemas.analyze import AnalyzeResponse, InstagramOgMeta
from app.services.crawler.instagram_reel import (
    instagram_media_type,
    is_instagram_media_url,
)
from app.services.crawler.instagram_reel_parse import parse_instagram_reel_meta
from app.services.crawler.playwright_service import fetch_page_content


async def run_analyze(url: str) -> AnalyzeResponse:
    settings = get_settings()
    try:
        html, text = await fetch_page_content(url, settings)
        if is_instagram_media_url(url) and text:
            parsed = parse_instagram_reel_meta(text)
            if parsed:
                mt = instagram_media_type(url)
                return AnalyzeResponse(
                    url=url,
                    success=True,
                    text=parsed["caption"],
                    html=html,
                    media_type=mt,
                    instagram=InstagramOgMeta(**parsed),
                )
        return AnalyzeResponse(url=url, success=True, text=text, html=html)
    except Exception as e:
        return AnalyzeResponse(
            url=url,
            success=False,
            error=str(e),
        )
