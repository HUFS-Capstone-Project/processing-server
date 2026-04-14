"""Playwright crawling entry points."""

from __future__ import annotations

import asyncio
import logging
import time

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from app.core.config import Settings
from app.services.crawler.instagram_context import (
    INSTAGRAM_BROWSER_ARGS,
    OG_EXTRACTION_JS,
    OG_READY_PREDICATE_JS,
    new_instagram_browser_context,
)
from app.services.crawler.instagram_reel import is_instagram_media_url

logger = logging.getLogger("processing.crawler.playwright")


def _browser_args(settings: Settings) -> list[str]:
    args: list[str] = []
    if settings.playwright_no_sandbox:
        args.append("--no-sandbox")
    if settings.playwright_disable_dev_shm_usage:
        args.append("--disable-dev-shm-usage")
    return args


async def _fetch_page_html_and_text(
    url: str,
    timeout_ms: int,
    settings: Settings,
) -> tuple[str | None, str]:
    started = time.monotonic()
    logger.info("crawler start mode=web url=%s timeout_ms=%s", url, timeout_ms)
    async with async_playwright() as p:
        logger.info("crawler playwright connected mode=web")
        browser = await p.chromium.launch(headless=True, args=_browser_args(settings))
        try:
            logger.info("crawler browser launched mode=web")
            page = await browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            html = await page.content()
            text = await page.evaluate(
                """() => {
                    const b = document.body;
                    return b ? b.innerText : '';
                }"""
            )
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                "crawler done mode=web url=%s elapsed_ms=%s text_len=%s",
                url,
                elapsed_ms,
                len(text or ""),
            )
            return html, text or ""
        finally:
            await browser.close()


async def _fetch_instagram_og_caption(
    url: str,
    navigation_timeout_ms: int,
    og_wait_timeout_ms: int,
    settings: Settings,
) -> tuple[str | None, str]:
    started = time.monotonic()
    logger.info(
        "crawler start mode=instagram url=%s navigation_timeout_ms=%s og_wait_timeout_ms=%s",
        url,
        navigation_timeout_ms,
        og_wait_timeout_ms,
    )
    launch_args = _browser_args(settings) + list(INSTAGRAM_BROWSER_ARGS)
    async with async_playwright() as p:
        logger.info("crawler playwright connected mode=instagram")
        browser = await p.chromium.launch(headless=True, args=launch_args)
        try:
            logger.info("crawler browser launched mode=instagram")
            context = await new_instagram_browser_context(browser, settings)
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=navigation_timeout_ms)
            try:
                await page.wait_for_function(
                    OG_READY_PREDICATE_JS,
                    timeout=max(0, og_wait_timeout_ms),
                )
            except PlaywrightTimeoutError:
                logger.info("crawler og wait timeout mode=instagram url=%s", url)
            caption = await page.evaluate(OG_EXTRACTION_JS)
            caption = (caption or "").strip()
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                "crawler done mode=instagram url=%s elapsed_ms=%s caption_len=%s",
                url,
                elapsed_ms,
                len(caption),
            )
            return None, caption
        finally:
            await browser.close()


async def fetch_page_content(url: str, settings: Settings) -> tuple[str | None, str]:
    nav_ms = max(1, settings.crawler_timeout) * 1000
    u = str(url)
    if is_instagram_media_url(u):
        instagram_nav_ms = max(1, settings.instagram_navigation_timeout) * 1000
        hard_timeout = max(
            5.0,
            (instagram_nav_ms + max(0, settings.instagram_og_wait_timeout_ms)) / 1000.0 + 10.0,
        )
        return await asyncio.wait_for(
            _fetch_instagram_og_caption(
                u,
                instagram_nav_ms,
                settings.instagram_og_wait_timeout_ms,
                settings,
            ),
            timeout=hard_timeout,
        )
    hard_timeout = max(5.0, nav_ms / 1000.0 + 10.0)
    return await asyncio.wait_for(_fetch_page_html_and_text(u, nav_ms, settings), timeout=hard_timeout)


async def fetch_page_html_and_text(url: str, settings: Settings) -> tuple[str | None, str]:
    return await fetch_page_content(url, settings)
