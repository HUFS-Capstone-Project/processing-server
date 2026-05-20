"""Playwright crawling entry points."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

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

logger = logging.getLogger("processing.crawler.playwright")


NAVER_BLOG_COMMON_CONTENT_SELECTORS = [
    ".se-main-container",
    "#postViewArea",
    ".se_component_wrap",
    ".post_ct",
    ".blog2_post",
    ".post-view",
    "article",
]

NAVER_BLOG_LIKE_COUNT_SELECTORS = [
    "span.u_likeit_text._count.num",
    ".u_likeit_text._count.num",
]

NAVER_BLOG_COMMENT_COUNT_SELECTORS = [
    "em#commentCount._commentCount",
    "#commentCount",
    "._commentCount",
]

NAVER_BLOG_POSTED_AT_SELECTORS = [
    ".blog2_container .se_publishDate",
    ".se_publishDate",
    ".blog2_container .date",
    ".se_publishDate.pcol2",
]

NAVER_BLOG_CONTENT_EXTRACTION_JS = """(selectors) => {
    for (const selector of selectors) {
        const node = document.querySelector(selector);
        if (!node) {
            continue;
        }
        const clone = node.cloneNode(true);
        clone.querySelectorAll('script, style, noscript').forEach((el) => el.remove());
        const text = clone.innerText || clone.textContent || '';
        if (text.trim()) {
            return { selector, text };
        }
    }
    return { selector: null, text: '' };
}"""

NAVER_BLOG_TEXT_BY_SELECTORS_JS = """(selectors) => {
    for (const selector of selectors) {
        const node = document.querySelector(selector);
        if (!node) {
            continue;
        }
        const text = node.innerText || node.textContent || '';
        if (text.trim()) {
            return text;
        }
    }
    return '';
}"""

NAVER_BLOG_IFRAME_SRC_JS = """() => {
    const frame = document.querySelector('iframe#mainFrame') ||
        document.querySelector('iframe[src*="PostView"]');
    return frame ? frame.getAttribute('src') : null;
}"""

INSTAGRAM_DIAGNOSTICS_JS = """() => {
    const html = document.documentElement ? document.documentElement.outerHTML : '';
    const bodyText = document.body ? (document.body.innerText || '') : '';
    const title = document.title || '';
    const ogNodes = Array.from(document.querySelectorAll('meta[property^="og:"], meta[name^="og:"]'));
    const hasOgDescription = Boolean(
        document.querySelector('meta[property="og:description"], meta[name="og:description"]')
    );
    const hasOgTitle = Boolean(
        document.querySelector('meta[property="og:title"], meta[name="og:title"]')
    );
    const loginFormPresent = Boolean(
        document.querySelector('form[action*="login"], input[name="username"], input[name="password"]')
    );
    const lower = `${title} ${bodyText}`.toLowerCase();
    const challengeMarkerPresent = lower.includes('checkpoint') ||
        lower.includes('challenge') ||
        lower.includes('login') ||
        lower.includes('sorry') ||
        lower.includes('try again later');
    return {
        title_len: title.length,
        html_len: html.length,
        body_text_len: bodyText.length,
        og_meta_count: ogNodes.length,
        og_description_present: hasOgDescription,
        og_title_present: hasOgTitle,
        login_form_present: loginFormPresent,
        challenge_marker_present: challengeMarkerPresent,
        empty_body: bodyText.trim().length === 0,
    };
}"""


@dataclass(slots=True)
class InstagramFetchResult:
    caption: str
    og_source: str
    og_wait_timed_out: bool
    early_extract_hit: bool
    blocked_resource_count: int
    launch_ms: int
    context_ms: int
    goto_ms: int
    og_wait_ms: int
    extract_ms: int
    total_ms: int
    response_status: int | None = None
    response_url: str | None = None
    final_url: str | None = None
    page_title_len: int = 0
    html_len: int = 0
    body_text_len: int = 0
    og_meta_count: int = 0
    og_description_present: bool = False
    og_title_present: bool = False
    login_form_present: bool = False
    challenge_marker_present: bool = False
    empty_body: bool = False


@dataclass(slots=True)
class NaverBlogFetchResult:
    html: str | None
    content_text: str
    resolved_url: str
    extraction_source: str
    selected_selector: str | None = None
    iframe_src: str | None = None
    log_no: str | None = None
    like_count_text: str | None = None
    comment_count_text: str | None = None
    posted_at_text: str | None = None


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


def safe_url_for_log(url: str | None) -> str | None:
    if not url:
        return None
    try:
        parsed = urlparse(str(url))
    except Exception:
        return "<invalid-url>"
    return parsed._replace(query="", fragment="").geturl()


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


def _parse_instagram_diagnostics(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    return {
        "page_title_len": _safe_int(raw.get("title_len")),
        "html_len": _safe_int(raw.get("html_len")),
        "body_text_len": _safe_int(raw.get("body_text_len")),
        "og_meta_count": _safe_int(raw.get("og_meta_count")),
        "og_description_present": bool(raw.get("og_description_present")),
        "og_title_present": bool(raw.get("og_title_present")),
        "login_form_present": bool(raw.get("login_form_present")),
        "challenge_marker_present": bool(raw.get("challenge_marker_present")),
        "empty_body": bool(raw.get("empty_body")),
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


async def _instagram_page_diagnostics(page) -> dict[str, Any]:
    try:
        return _parse_instagram_diagnostics(await page.evaluate(INSTAGRAM_DIAGNOSTICS_JS))
    except Exception:
        logger.debug("instagram diagnostics evaluation failed", exc_info=True)
        return {}


def _instagram_result_metadata(result: InstagramFetchResult) -> dict[str, Any]:
    return {
        "extraction_source": "instagram_og_meta",
        "response_status": result.response_status,
        "response_url": safe_url_for_log(result.response_url),
        "final_url": safe_url_for_log(result.final_url),
        "html_len": result.html_len,
        "body_text_len": result.body_text_len,
        "empty_body": result.empty_body,
        "page_title_len": result.page_title_len,
        "instagram": {
            "og_source": result.og_source,
            "og_meta_count": result.og_meta_count,
            "og_title_present": result.og_title_present,
            "og_description_present": result.og_description_present,
            "early_extract_hit": result.early_extract_hit,
            "og_wait_timed_out": result.og_wait_timed_out,
            "login_form_present": result.login_form_present,
            "challenge_marker_present": result.challenge_marker_present,
            "blocked_resource_count": result.blocked_resource_count,
        },
    }


def instagram_fetch_metadata(result: InstagramFetchResult) -> dict[str, Any]:
    return _instagram_result_metadata(result)


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


def _has_meaningful_og_payload(og_source: str, caption: str) -> bool:
    if caption.strip():
        return True
    return og_source not in {"", "none"}


async def _extract_og_from_page(page) -> tuple[str, str, int]:
    extract_started = time.monotonic()
    raw = await page.evaluate(OG_EXTRACTION_JS)
    og_source, caption = _parse_og_extraction_result(raw)
    extract_ms = int((time.monotonic() - extract_started) * 1000)
    return og_source, caption, extract_ms


async def _build_instagram_fetch_result(
    *,
    page,
    response,
    caption: str,
    og_source: str,
    og_wait_timed_out: bool,
    early_extract_hit: bool,
    blocked_resource_count: int,
    launch_ms: int,
    context_ms: int,
    goto_ms: int,
    og_wait_ms: int,
    extract_ms: int,
    total_ms: int,
) -> InstagramFetchResult:
    diagnostics = await _instagram_page_diagnostics(page)
    return InstagramFetchResult(
        caption=caption,
        og_source=og_source,
        og_wait_timed_out=og_wait_timed_out,
        early_extract_hit=early_extract_hit,
        blocked_resource_count=blocked_resource_count,
        launch_ms=launch_ms,
        context_ms=context_ms,
        goto_ms=goto_ms,
        og_wait_ms=og_wait_ms,
        extract_ms=extract_ms,
        total_ms=total_ms,
        response_status=response.status if response is not None else None,
        response_url=response.url if response is not None else None,
        final_url=getattr(page, "url", None),
        page_title_len=diagnostics.get("page_title_len", 0),
        html_len=diagnostics.get("html_len", 0),
        body_text_len=diagnostics.get("body_text_len", 0),
        og_meta_count=diagnostics.get("og_meta_count", 0),
        og_description_present=diagnostics.get("og_description_present", False),
        og_title_present=diagnostics.get("og_title_present", False),
        login_form_present=diagnostics.get("login_form_present", False),
        challenge_marker_present=diagnostics.get("challenge_marker_present", False),
        empty_body=diagnostics.get("empty_body", False),
    )


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
        response = await page.goto(url, wait_until="domcontentloaded", timeout=navigation_timeout_ms)
        goto_ms = int((time.monotonic() - goto_started) * 1000)

        og_source, caption, extract_ms = await _extract_og_from_page(page)
        if _has_meaningful_og_payload(og_source, caption):
            total_ms = int((time.monotonic() - started) * 1000)
            return await _build_instagram_fetch_result(
                page=page,
                response=response,
                caption=caption,
                og_source=og_source,
                og_wait_timed_out=False,
                early_extract_hit=True,
                blocked_resource_count=route_stats.blocked_resource_count,
                launch_ms=launch_ms,
                context_ms=context_ms,
                goto_ms=goto_ms,
                og_wait_ms=0,
                extract_ms=extract_ms,
                total_ms=total_ms,
            )

        og_wait_started = time.monotonic()
        try:
            await page.wait_for_function(
                OG_READY_PREDICATE_JS,
                timeout=max(0, og_wait_timeout_ms),
            )
        except PlaywrightTimeoutError:
            og_wait_timed_out = True
            logger.info("crawler og wait timeout mode=instagram url=%s", safe_url_for_log(url))
        og_wait_ms = int((time.monotonic() - og_wait_started) * 1000)

        og_source_after_wait, caption_after_wait, extract_after_wait_ms = await _extract_og_from_page(page)
        og_source = og_source_after_wait or og_source
        caption = caption_after_wait or caption
        extract_ms += extract_after_wait_ms
        total_ms = int((time.monotonic() - started) * 1000)
        return await _build_instagram_fetch_result(
            page=page,
            response=response,
            caption=caption,
            og_source=og_source,
            og_wait_timed_out=og_wait_timed_out,
            early_extract_hit=False,
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


async def _fetch_instagram_og_result(
    url: str,
    navigation_timeout_ms: int,
    og_wait_timeout_ms: int,
    settings: Settings,
) -> InstagramFetchResult:
    logger.info(
        "crawler start mode=instagram url=%s navigation_timeout_ms=%s og_wait_timeout_ms=%s reuse=%s blocked_types=%s",
        safe_url_for_log(url),
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
                    "og_source=%s og_wait_timed_out=%s early_extract_hit=%s "
                    "blocked_resource_count=%s caption_len=%s response_status=%s "
                    "response_url=%s final_url=%s html_len=%s body_text_len=%s "
                    "og_meta_count=%s og_description_present=%s og_title_present=%s "
                    "login_form_present=%s challenge_marker_present=%s empty_body=%s"
                ),
                safe_url_for_log(url),
                fetch_result.total_ms,
                fetch_result.launch_ms,
                fetch_result.context_ms,
                fetch_result.goto_ms,
                fetch_result.og_wait_ms,
                fetch_result.extract_ms,
                fetch_result.og_source,
                fetch_result.og_wait_timed_out,
                fetch_result.early_extract_hit,
                fetch_result.blocked_resource_count,
                len(fetch_result.caption),
                fetch_result.response_status,
                safe_url_for_log(fetch_result.response_url),
                safe_url_for_log(fetch_result.final_url),
                fetch_result.html_len,
                fetch_result.body_text_len,
                fetch_result.og_meta_count,
                fetch_result.og_description_present,
                fetch_result.og_title_present,
                fetch_result.login_form_present,
                fetch_result.challenge_marker_present,
                fetch_result.empty_body,
            )
            return fetch_result
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
                safe_url_for_log(url),
                attempt + 1,
                exc.__class__.__name__,
            )
            await _INSTAGRAM_RUNTIME.shutdown()
    if last_error:
        raise last_error
    return InstagramFetchResult(
        caption="",
        og_source="none",
        og_wait_timed_out=False,
        early_extract_hit=False,
        blocked_resource_count=0,
        launch_ms=0,
        context_ms=0,
        goto_ms=0,
        og_wait_ms=0,
        extract_ms=0,
        total_ms=0,
    )


async def _fetch_instagram_og_caption(
    url: str,
    navigation_timeout_ms: int,
    og_wait_timeout_ms: int,
    settings: Settings,
) -> tuple[str | None, str]:
    result = await _fetch_instagram_og_result(
        url,
        navigation_timeout_ms,
        og_wait_timeout_ms,
        settings,
    )
    return None, result.caption


def extract_naver_blog_log_no(url: str) -> str | None:
    try:
        parsed = urlparse(str(url))
    except Exception:
        return None

    query_log_no = (parse_qs(parsed.query).get("logNo") or [None])[0]
    if query_log_no:
        return str(query_log_no).strip() or None

    parts = [part for part in (parsed.path or "").split("/") if part]
    if len(parts) >= 2 and parts[-1].lower() != "postview.naver":
        return parts[-1].strip() or None
    return None


def naver_blog_content_selectors(log_no: str | None) -> list[str]:
    selectors: list[str] = []
    if log_no and re.fullmatch(r"[A-Za-z0-9_-]+", log_no):
        selectors.extend(
            [
                f"#post-view{log_no} > div > div.se-main-container",
                f"#post-view{log_no} .se-main-container",
                f"#post-view{log_no}",
            ]
        )
    selectors.extend(NAVER_BLOG_COMMON_CONTENT_SELECTORS)
    return selectors


def normalize_naver_blog_text(text: str) -> str:
    normalized = (text or "").replace("\u00a0", " ")
    normalized = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", normalized)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in normalized.splitlines()]

    compacted: list[str] = []
    blank_seen = False
    for line in lines:
        if not line:
            if compacted and not blank_seen:
                compacted.append("")
                blank_seen = True
            continue
        compacted.append(line)
        blank_seen = False

    while compacted and not compacted[-1]:
        compacted.pop()
    return "\n".join(compacted).strip()


def _parse_naver_content_result(raw: Any) -> tuple[str | None, str]:
    if not isinstance(raw, dict):
        return None, ""
    selector = raw.get("selector")
    text = raw.get("text")
    clean_selector = str(selector).strip() if selector else None
    return clean_selector or None, str(text or "")


def _is_naver_post_view_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url))
    except Exception:
        return False
    return "postview" in (parsed.path or "").lower()


async def _extract_naver_blog_from_page(
    page,
    *,
    log_no: str | None,
    extraction_source: str,
    iframe_src: str | None,
) -> NaverBlogFetchResult:
    selectors = naver_blog_content_selectors(log_no)
    selected_selector, raw_text = _parse_naver_content_result(
        await page.evaluate(NAVER_BLOG_CONTENT_EXTRACTION_JS, selectors)
    )
    try:
        await page.wait_for_selector(
            ", ".join(
                NAVER_BLOG_LIKE_COUNT_SELECTORS
                + NAVER_BLOG_COMMENT_COUNT_SELECTORS
                + NAVER_BLOG_POSTED_AT_SELECTORS
            ),
            timeout=1000,
        )
    except PlaywrightTimeoutError:
        logger.info("crawler naver_blog metadata selector wait timeout url=%s", safe_url_for_log(page.url))
    like_count_text = str(
        await page.evaluate(NAVER_BLOG_TEXT_BY_SELECTORS_JS, NAVER_BLOG_LIKE_COUNT_SELECTORS) or ""
    ).strip()
    comment_count_text = str(
        await page.evaluate(NAVER_BLOG_TEXT_BY_SELECTORS_JS, NAVER_BLOG_COMMENT_COUNT_SELECTORS) or ""
    ).strip()
    posted_at_text = str(
        await page.evaluate(NAVER_BLOG_TEXT_BY_SELECTORS_JS, NAVER_BLOG_POSTED_AT_SELECTORS) or ""
    ).strip()
    html = await page.content()
    return NaverBlogFetchResult(
        html=html,
        content_text=normalize_naver_blog_text(raw_text),
        resolved_url=page.url,
        extraction_source=extraction_source,
        selected_selector=selected_selector,
        iframe_src=iframe_src,
        log_no=log_no,
        like_count_text=like_count_text or None,
        comment_count_text=comment_count_text or None,
        posted_at_text=posted_at_text or None,
    )


async def _fetch_naver_blog_page_content(
    url: str,
    timeout_ms: int,
    settings: Settings,
) -> NaverBlogFetchResult:
    started = time.monotonic()
    logger.info("crawler start mode=naver_blog url=%s timeout_ms=%s", safe_url_for_log(url), timeout_ms)
    async with async_playwright() as p:
        logger.info("crawler playwright connected mode=naver_blog")
        browser = await p.chromium.launch(headless=True, args=_browser_args(settings))
        try:
            logger.info("crawler browser launched mode=naver_blog")
            page = await browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            log_no = extract_naver_blog_log_no(page.url) or extract_naver_blog_log_no(url)
            iframe_src = await page.evaluate(NAVER_BLOG_IFRAME_SRC_JS)
            if iframe_src:
                iframe_url = urljoin(page.url, str(iframe_src))
                await page.goto(iframe_url, wait_until="domcontentloaded", timeout=timeout_ms)
                log_no = extract_naver_blog_log_no(page.url) or log_no
                result = await _extract_naver_blog_from_page(
                    page,
                    log_no=log_no,
                    extraction_source="iframe_post_view",
                    iframe_src=str(iframe_src),
                )
            else:
                result = await _extract_naver_blog_from_page(
                    page,
                    log_no=log_no,
                    extraction_source=(
                        "post_view" if _is_naver_post_view_url(page.url) else "direct_selector"
                    ),
                    iframe_src=None,
                )

            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                (
                    "crawler done mode=naver_blog url=%s resolved_url=%s elapsed_ms=%s "
                    "source=%s selector=%s text_len=%s like_text_found=%s "
                    "comment_text_found=%s posted_at_text_found=%s"
                ),
                safe_url_for_log(url),
                safe_url_for_log(result.resolved_url),
                elapsed_ms,
                result.extraction_source,
                result.selected_selector,
                len(result.content_text),
                bool(result.like_count_text),
                bool(result.comment_count_text),
                bool(result.posted_at_text),
            )
            return result
        finally:
            await browser.close()


async def fetch_instagram_media_content(url: str, settings: Settings) -> tuple[str | None, str]:
    result = await fetch_instagram_media_result(url, settings)
    return None, result.caption


async def fetch_instagram_media_result(url: str, settings: Settings) -> InstagramFetchResult:
    u = str(url)
    instagram_nav_ms = max(1, settings.instagram_navigation_timeout) * 1000
    hard_timeout = max(
        5.0,
        (instagram_nav_ms + max(0, settings.instagram_og_wait_timeout_ms)) / 1000.0
        + max(0.0, settings.crawler_hard_timeout_margin_seconds),
    )
    return await asyncio.wait_for(
        _fetch_instagram_og_result(
            u,
            instagram_nav_ms,
            settings.instagram_og_wait_timeout_ms,
            settings,
        ),
        timeout=hard_timeout,
    )


async def fetch_generic_web_content(url: str, settings: Settings) -> tuple[str | None, str]:
    nav_ms = max(1, settings.crawler_timeout) * 1000
    hard_timeout = max(5.0, nav_ms / 1000.0 + max(0.0, settings.crawler_hard_timeout_margin_seconds))
    return await asyncio.wait_for(_fetch_page_html_and_text(str(url), nav_ms, settings), timeout=hard_timeout)


async def fetch_naver_blog_content(url: str, settings: Settings) -> NaverBlogFetchResult:
    nav_ms = max(1, settings.crawler_timeout) * 1000
    hard_timeout = max(5.0, nav_ms / 1000.0 + max(0.0, settings.crawler_hard_timeout_margin_seconds))
    return await asyncio.wait_for(
        _fetch_naver_blog_page_content(str(url), nav_ms, settings),
        timeout=hard_timeout,
    )


async def shutdown_crawler_runtime() -> None:
    await _INSTAGRAM_RUNTIME.shutdown()


async def prewarm_crawler_runtime(settings: Settings) -> bool:
    if not settings.crawler_browser_reuse_enabled:
        return False
    started = time.monotonic()
    try:
        async with _INSTAGRAM_RUNTIME.slot():
            browser, launch_ms = await _INSTAGRAM_RUNTIME.ensure_browser(settings)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.info(
            "crawler prewarm done launch_ms=%s elapsed_ms=%s browser_connected=%s",
            launch_ms,
            elapsed_ms,
            browser.is_connected(),
        )
        return True
    except Exception:
        logger.warning("crawler prewarm failed", exc_info=True)
        return False
