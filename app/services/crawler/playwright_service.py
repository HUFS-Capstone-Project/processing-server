"""Playwright로 페이지 fetch. Instagram은 og 문자열만, 그 외는 HTML+본문."""

import asyncio

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from app.core.config import Settings
from app.services.crawler.instagram_context import (
    INSTAGRAM_BROWSER_ARGS,
    OG_EXTRACTION_JS,
    OG_READY_PREDICATE_JS,
    new_instagram_browser_context,
)
from app.services.crawler.instagram_reel import is_instagram_media_url


def _fetch_page_html_and_text_sync(url: str, timeout_ms: int) -> tuple[str | None, str]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            html = page.content()
            text = page.evaluate(
                """() => {
                    const b = document.body;
                    return b ? b.innerText : '';
                }"""
            )
            return html, text or ""
        finally:
            browser.close()


def _fetch_instagram_og_caption_sync(
    url: str,
    navigation_timeout_ms: int,
    og_wait_timeout_ms: int,
    settings: Settings,
) -> tuple[str | None, str]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=list(INSTAGRAM_BROWSER_ARGS))
        try:
            context = new_instagram_browser_context(browser, settings)
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=navigation_timeout_ms)
            try:
                page.wait_for_function(
                    OG_READY_PREDICATE_JS,
                    timeout=max(0, og_wait_timeout_ms),
                )
            except PlaywrightTimeoutError:
                pass
            caption = page.evaluate(OG_EXTRACTION_JS)
            caption = (caption or "").strip()
            return None, caption
        finally:
            browser.close()


async def fetch_page_content(url: str, settings: Settings) -> tuple[str | None, str]:
    nav_ms = max(1, settings.crawler_timeout) * 1000
    u = str(url)
    if is_instagram_media_url(u):
        instagram_nav_ms = max(1, settings.instagram_navigation_timeout) * 1000
        return await asyncio.to_thread(
            _fetch_instagram_og_caption_sync,
            u,
            instagram_nav_ms,
            settings.instagram_og_wait_timeout_ms,
            settings,
        )
    return await asyncio.to_thread(_fetch_page_html_and_text_sync, u, nav_ms)


async def fetch_page_html_and_text(url: str, settings: Settings) -> tuple[str | None, str]:
    return await fetch_page_content(url, settings)
