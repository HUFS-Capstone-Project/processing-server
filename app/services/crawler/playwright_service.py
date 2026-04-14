"""Playwright crawling entry points."""

from __future__ import annotations

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


def _browser_args(settings: Settings) -> list[str]:
    args: list[str] = []
    if settings.playwright_no_sandbox:
        args.append("--no-sandbox")
    if settings.playwright_disable_dev_shm_usage:
        args.append("--disable-dev-shm-usage")
    return args


def _fetch_page_html_and_text_sync(url: str, timeout_ms: int, settings: Settings) -> tuple[str | None, str]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=_browser_args(settings))
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
    launch_args = _browser_args(settings) + list(INSTAGRAM_BROWSER_ARGS)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=launch_args)
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
    return await asyncio.to_thread(_fetch_page_html_and_text_sync, u, nav_ms, settings)


async def fetch_page_html_and_text(url: str, settings: Settings) -> tuple[str | None, str]:
    return await fetch_page_content(url, settings)
