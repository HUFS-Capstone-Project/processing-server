"""Playwright crawling entry points."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from playwright.async_api import Browser
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Playwright
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from app.core.config import Settings
from app.services.crawler.instagram_context import (
    INSTAGRAM_BROWSER_ARGS,
    OG_EXTRACTION_JS,
    OG_READY_PREDICATE_JS,
    configure_instagram_page,
    new_instagram_browser_context,
)
from app.services.crawler.instagram_reel import is_instagram_media_url

logger = logging.getLogger("processing.crawler.playwright")


@dataclass(slots=True)
class InstagramFetchResult:
    caption: str
    og_source: str
    og_wait_timed_out: bool
    blocked_resource_count: int
    launch_ms: int
    context_ms: int
    goto_ms: int
    og_wait_ms: int
    extract_ms: int
    total_ms: int


class _InstagramCrawlerRuntime:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(1)
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None

    @asynccontextmanager
    async def slot(self):
        async with self._semaphore:
            yield

    async def ensure_browser(self, settings: Settings) -> tuple[Browser, int]:
        async with self._lock:
            if self._browser and self._browser.is_connected():
                return self._browser, 0
            await self._shutdown_unlocked()
            launch_args = _browser_args(settings) + list(INSTAGRAM_BROWSER_ARGS)
            started = time.monotonic()
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True, args=launch_args)
            launch_ms = int((time.monotonic() - started) * 1000)
            return self._browser, launch_ms

    async def shutdown(self) -> None:
        async with self._lock:
            await self._shutdown_unlocked()

    async def _shutdown_unlocked(self) -> None:
        browser = self._browser
        playwright = self._playwright
        self._browser = None
        self._playwright = None
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                logger.debug("crawler browser close failed", exc_info=True)
        if playwright is not None:
            try:
                await playwright.stop()
            except Exception:
                logger.debug("crawler playwright stop failed", exc_info=True)


_INSTAGRAM_RUNTIME = _InstagramCrawlerRuntime()


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


def _parse_og_extraction_result(raw: Any) -> tuple[str, str]:
    if isinstance(raw, dict):
        source = str(raw.get("source") or "none").strip() or "none"
        content = str(raw.get("content") or "").strip()
        return source, content
    if isinstance(raw, str):
        content = raw.strip()
        return ("none" if not content else "unknown"), content
    return "none", ""


def _is_browser_crash_error(exc: Exception) -> bool:
    if isinstance(exc, PlaywrightTimeoutError):
        return False
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return False
    if not isinstance(exc, PlaywrightError):
        return False
    msg = str(exc).lower()
    markers = (
        "browser has been closed",
        "target page, context or browser has been closed",
        "connection closed",
        "has been terminated",
    )
    return any(marker in msg for marker in markers)


async def _run_instagram_fetch_with_browser(
    *,
    browser: Browser,
    launch_ms: int,
    url: str,
    navigation_timeout_ms: int,
    og_wait_timeout_ms: int,
    settings: Settings,
) -> InstagramFetchResult:
    started = time.monotonic()
    context_started = time.monotonic()
    context = await new_instagram_browser_context(browser, settings)
    context_ms = int((time.monotonic() - context_started) * 1000)
    page = await context.new_page()
    route_stats = await configure_instagram_page(page, settings)
    og_wait_timed_out = False
    try:
        goto_started = time.monotonic()
        await page.goto(url, wait_until="domcontentloaded", timeout=navigation_timeout_ms)
        goto_ms = int((time.monotonic() - goto_started) * 1000)

        og_wait_started = time.monotonic()
        try:
            await page.wait_for_function(
                OG_READY_PREDICATE_JS,
                timeout=max(0, og_wait_timeout_ms),
            )
        except PlaywrightTimeoutError:
            og_wait_timed_out = True
            logger.info("crawler og wait timeout mode=instagram url=%s", url)
        og_wait_ms = int((time.monotonic() - og_wait_started) * 1000)

        extract_started = time.monotonic()
        raw = await page.evaluate(OG_EXTRACTION_JS)
        og_source, caption = _parse_og_extraction_result(raw)
        extract_ms = int((time.monotonic() - extract_started) * 1000)
        total_ms = int((time.monotonic() - started) * 1000)
        return InstagramFetchResult(
            caption=caption,
            og_source=og_source,
            og_wait_timed_out=og_wait_timed_out,
            blocked_resource_count=route_stats.blocked_resource_count,
            launch_ms=launch_ms,
            context_ms=context_ms,
            goto_ms=goto_ms,
            og_wait_ms=og_wait_ms,
            extract_ms=extract_ms,
            total_ms=total_ms,
        )
    finally:
        await context.close()


async def _fetch_instagram_og_caption(
    url: str,
    navigation_timeout_ms: int,
    og_wait_timeout_ms: int,
    settings: Settings,
) -> tuple[str | None, str]:
    logger.info(
        "crawler start mode=instagram url=%s navigation_timeout_ms=%s og_wait_timeout_ms=%s reuse=%s blocked_types=%s",
        url,
        navigation_timeout_ms,
        og_wait_timeout_ms,
        settings.crawler_browser_reuse_enabled,
        sorted(settings.instagram_block_resource_type_set),
    )
    recoverable = bool(settings.crawler_recover_on_browser_crash)
    retries = 1 if recoverable else 0
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            if settings.crawler_browser_reuse_enabled:
                async with _INSTAGRAM_RUNTIME.slot():
                    browser, launch_ms = await _INSTAGRAM_RUNTIME.ensure_browser(settings)
                    fetch_result = await _run_instagram_fetch_with_browser(
                        browser=browser,
                        launch_ms=launch_ms,
                        url=url,
                        navigation_timeout_ms=navigation_timeout_ms,
                        og_wait_timeout_ms=og_wait_timeout_ms,
                        settings=settings,
                    )
            else:
                launch_args = _browser_args(settings) + list(INSTAGRAM_BROWSER_ARGS)
                launch_started = time.monotonic()
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True, args=launch_args)
                    launch_ms = int((time.monotonic() - launch_started) * 1000)
                    try:
                        fetch_result = await _run_instagram_fetch_with_browser(
                            browser=browser,
                            launch_ms=launch_ms,
                            url=url,
                            navigation_timeout_ms=navigation_timeout_ms,
                            og_wait_timeout_ms=og_wait_timeout_ms,
                            settings=settings,
                        )
                    finally:
                        await browser.close()
            logger.info(
                (
                    "crawler done mode=instagram url=%s total_ms=%s launch_ms=%s "
                    "context_ms=%s goto_ms=%s og_wait_ms=%s extract_ms=%s "
                    "og_source=%s og_wait_timed_out=%s blocked_resource_count=%s caption_len=%s"
                ),
                url,
                fetch_result.total_ms,
                fetch_result.launch_ms,
                fetch_result.context_ms,
                fetch_result.goto_ms,
                fetch_result.og_wait_ms,
                fetch_result.extract_ms,
                fetch_result.og_source,
                fetch_result.og_wait_timed_out,
                fetch_result.blocked_resource_count,
                len(fetch_result.caption),
            )
            return None, fetch_result.caption
        except Exception as exc:
            last_error = exc
            is_recoverable = (
                settings.crawler_browser_reuse_enabled
                and settings.crawler_recover_on_browser_crash
                and _is_browser_crash_error(exc)
                and attempt < retries
            )
            if not is_recoverable:
                raise
            logger.warning(
                "crawler browser crash recovered mode=instagram url=%s attempt=%s error=%s",
                url,
                attempt + 1,
                exc.__class__.__name__,
            )
            await _INSTAGRAM_RUNTIME.shutdown()
    if last_error:
        raise last_error
    return None, ""


async def fetch_page_content(url: str, settings: Settings) -> tuple[str | None, str]:
    nav_ms = max(1, settings.crawler_timeout) * 1000
    u = str(url)
    if is_instagram_media_url(u):
        instagram_nav_ms = max(1, settings.instagram_navigation_timeout) * 1000
        hard_timeout = max(
            5.0,
            (instagram_nav_ms + max(0, settings.instagram_og_wait_timeout_ms)) / 1000.0
            + max(0.0, settings.crawler_hard_timeout_margin_seconds),
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
    hard_timeout = max(5.0, nav_ms / 1000.0 + max(0.0, settings.crawler_hard_timeout_margin_seconds))
    return await asyncio.wait_for(_fetch_page_html_and_text(u, nav_ms, settings), timeout=hard_timeout)


async def fetch_page_html_and_text(url: str, settings: Settings) -> tuple[str | None, str]:
    return await fetch_page_content(url, settings)


async def shutdown_crawler_runtime() -> None:
    await _INSTAGRAM_RUNTIME.shutdown()
