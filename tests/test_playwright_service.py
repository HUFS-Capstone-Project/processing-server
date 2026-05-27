from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from playwright.async_api import Error as PlaywrightError

from app.core.config import Settings
from app.services.crawler import playwright_service as service


if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _can_create_event_loop() -> bool:
    try:
        loop = asyncio.new_event_loop()
        loop.close()
        return True
    except OSError:
        return False


EVENT_LOOP_AVAILABLE = _can_create_event_loop()


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        coro.close()
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _run_with_subprocesses(coro):
    if not hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        return _run(coro)
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        return asyncio.run(coro)
    finally:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class _FakeRuntime:
    class _BrowserStub:
        @staticmethod
        def is_connected() -> bool:
            return True

    def __init__(self) -> None:
        self.ensure_calls = 0
        self.launch_count = 0
        self.shutdown_calls = 0
        self._launched = False

    @asynccontextmanager
    async def slot(self):
        yield

    async def ensure_browser(self, _settings: Settings):
        self.ensure_calls += 1
        if not self._launched:
            self._launched = True
            self.launch_count += 1
            return self._BrowserStub(), 17
        return self._BrowserStub(), 0

    async def shutdown(self) -> None:
        self.shutdown_calls += 1
        self._launched = False


class _FakeInstagramImagePage:
    def __init__(self) -> None:
        self.step = 0

    async def evaluate(self, script: str):
        assert script == service.INSTAGRAM_IMAGE_URL_EXTRACTION_JS
        if self.step == 0:
            return [
                "https://scontent.cdninstagram.com/v/t51.29350-15/one.jpg?x=1#frag",
                "https://scontent.cdninstagram.com/v/t51.29350-15/one.jpg?x=1#frag",
            ]
        return [
            "https://scontent.cdninstagram.com/v/t51.29350-15/two.jpg?x=2",
        ]

    def locator(self, _selector: str):
        return _FakeInstagramNextLocator(self)

    async def wait_for_timeout(self, _timeout_ms: int):
        return None


class _FakeInstagramNextLocator:
    def __init__(self, page: _FakeInstagramImagePage) -> None:
        self._page = page

    @property
    def first(self):
        return self

    async def count(self) -> int:
        return 1 if self._page.step == 0 else 0

    async def is_visible(self) -> bool:
        return self._page.step == 0

    async def click(self, timeout: int):
        assert timeout == 1000
        self._page.step += 1


def test_naver_blog_log_no_extraction_from_path_and_query() -> None:
    assert service.extract_naver_blog_log_no("https://blog.naver.com/example/123") == "123"
    assert (
        service.extract_naver_blog_log_no(
            "https://blog.naver.com/PostView.naver?blogId=example&logNo=456"
        )
        == "456"
    )


def test_naver_blog_selector_priority_uses_log_no_specific_selectors_first() -> None:
    selectors = service.naver_blog_content_selectors("123")

    assert selectors[:4] == [
        "#post-view123 > div > div.se-main-container",
        "#post-view123 .se-main-container",
        "#post-view123",
        ".se-main-container",
    ]


def test_naver_blog_text_normalization_removes_zero_width_and_compacts_blank_lines() -> None:
    text = "  hello\u200b\u00a0 world  \n\n\n  second\t\tline  \n"

    assert service.normalize_naver_blog_text(text) == "hello world\n\nsecond line"


def test_instagram_image_url_normalization_deduplicates_and_limits() -> None:
    urls = service._normalize_instagram_image_urls(
        [
            "https://scontent.cdninstagram.com/a.jpg?x=1#fragment",
            "https://scontent.cdninstagram.com/a.jpg?x=1#fragment",
            "data:image/png;base64,abc",
            "not-a-url",
            "https://scontent.cdninstagram.com/v/t51.2885-19/profile.jpg?stp=dst-jpg_s150x150",
            "https://scontent.cdninstagram.com/b.webp",
        ],
        max_images=1,
    )

    assert urls == ["https://scontent.cdninstagram.com/a.jpg?x=1"]


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_collect_instagram_post_image_urls_clicks_carousel_next() -> None:
    page = _FakeInstagramImagePage()

    urls, next_clicks, collection_ms = _run(
        service._collect_instagram_post_image_urls(
            page,
            Settings(instagram_image_fetch_max_images=5, instagram_image_fetch_max_next_clicks=3),
        )
    )

    assert urls == [
        "https://scontent.cdninstagram.com/v/t51.29350-15/one.jpg?x=1",
        "https://scontent.cdninstagram.com/v/t51.29350-15/two.jpg?x=2",
    ]
    assert next_clicks == 1
    assert collection_ms >= 0


def test_instagram_fetch_metadata_sanitizes_diagnostic_urls() -> None:
    result = service.InstagramFetchResult(
        caption="",
        og_source="none",
        og_wait_timed_out=True,
        early_extract_hit=False,
        blocked_resource_count=0,
        launch_ms=1,
        context_ms=1,
        goto_ms=1,
        og_wait_ms=1,
        extract_ms=1,
        total_ms=5,
        response_url="https://www.instagram.com/reel/abc/?token=secret#fragment",
        final_url="https://www.instagram.com/accounts/login/?next=/reel/abc/&token=secret",
    )

    metadata = service.instagram_fetch_metadata(result)

    assert metadata["response_url"] == "https://www.instagram.com/reel/abc/"
    assert metadata["final_url"] == "https://www.instagram.com/accounts/login/"
    assert metadata["instagram"]["og_source"] == "none"
    assert "caption" not in metadata


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_naver_blog_page_extraction_returns_selector_text_and_like_text() -> None:
    class FakePage:
        url = "https://blog.naver.com/PostView.naver?blogId=example&logNo=123"

        def __init__(self) -> None:
            self.selector_args: list[list[str]] = []

        async def evaluate(self, script: str, arg):
            if script == service.NAVER_BLOG_CONTENT_EXTRACTION_JS:
                self.selector_args.append(arg)
                return {
                    "selector": "#post-view123 > div > div.se-main-container",
                    "text": "  body\u200b text  ",
                }
            if script == service.NAVER_BLOG_TEXT_BY_SELECTORS_JS:
                return "좋아요 4"
            raise AssertionError(f"unexpected script: {script}")

        async def wait_for_selector(self, selector: str, timeout: int):
            self.wait_selector = selector
            self.wait_timeout = timeout
            return object()

        async def content(self) -> str:
            return "<html>raw</html>"

    page = FakePage()

    result = _run(
        service._extract_naver_blog_from_page(
            page,
            log_no="123",
            extraction_source="iframe_post_view",
            iframe_src="/PostView.naver?blogId=example&logNo=123",
        )
    )

    assert page.selector_args[0][0] == "#post-view123 > div > div.se-main-container"
    assert result.content_text == "body text"
    assert result.like_count_text == "좋아요 4"
    assert result.html == "<html>raw</html>"


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_instagram_fetch_reuses_browser_launch(monkeypatch) -> None:
    runtime = _FakeRuntime()
    monkeypatch.setattr(service, "_INSTAGRAM_RUNTIME", runtime)

    async def fake_run(**_kwargs):
        return service.InstagramFetchResult(
            caption="ok",
            og_source="og:description",
            og_wait_timed_out=False,
            early_extract_hit=False,
            blocked_resource_count=3,
            launch_ms=0,
            context_ms=4,
            goto_ms=20,
            og_wait_ms=5,
            extract_ms=3,
            total_ms=32,
        )

    monkeypatch.setattr(service, "_run_instagram_fetch_with_browser", fake_run)

    settings = Settings(crawler_browser_reuse_enabled=True, crawler_recover_on_browser_crash=True)
    _run(service._fetch_instagram_og_caption("https://www.instagram.com/reel/abc/", 12000, 3000, settings))
    _run(service._fetch_instagram_og_caption("https://www.instagram.com/reel/def/", 12000, 3000, settings))

    assert runtime.ensure_calls == 2
    assert runtime.launch_count == 1


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_instagram_fetch_returns_429_without_waiting_for_og(monkeypatch) -> None:
    class FakeResponse:
        status = 429
        url = "https://www.instagram.com/reel/abc/"

    class FakePage:
        url = "https://www.instagram.com/reel/abc/"
        wait_for_function_called = False

        async def goto(self, *_args, **_kwargs):
            return FakeResponse()

        async def wait_for_function(self, *_args, **_kwargs):
            self.wait_for_function_called = True
            raise AssertionError("OG wait should not run for Instagram 429")

        async def evaluate(self, script: str):
            if script == service.INSTAGRAM_DIAGNOSTICS_JS:
                return {
                    "html_len": 256,
                    "body_text_len": 0,
                    "og_meta_count": 0,
                    "empty_body": True,
                }
            raise AssertionError("OG extraction should not run for Instagram 429")

    class FakeContext:
        def __init__(self, page: FakePage) -> None:
            self.page = page

        async def new_page(self):
            return self.page

        async def close(self):
            return None

    page = FakePage()

    async def fake_context(_browser, _settings):
        return FakeContext(page)

    async def fake_configure_page(_page, _settings):
        return SimpleNamespace(blocked_resource_count=0)

    monkeypatch.setattr(service, "new_instagram_browser_context", fake_context)
    monkeypatch.setattr(service, "configure_instagram_page", fake_configure_page)

    result = _run(
        service._run_instagram_fetch_with_browser(
            browser=object(),
            launch_ms=0,
            url="https://www.instagram.com/reel/abc/",
            navigation_timeout_ms=12000,
            og_wait_timeout_ms=3000,
            settings=Settings(),
        )
    )

    assert result.response_status == 429
    assert result.og_wait_ms == 0
    assert result.extract_ms == 0
    assert result.og_wait_timed_out is False
    assert page.wait_for_function_called is False


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_instagram_fetch_recovers_on_browser_crash_once(monkeypatch) -> None:
    runtime = _FakeRuntime()
    monkeypatch.setattr(service, "_INSTAGRAM_RUNTIME", runtime)

    calls = {"count": 0}

    async def fake_run(**_kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise PlaywrightError("Browser has been closed")
        return service.InstagramFetchResult(
            caption="recovered",
            og_source="og:description",
            og_wait_timed_out=False,
            early_extract_hit=False,
            blocked_resource_count=0,
            launch_ms=0,
            context_ms=1,
            goto_ms=1,
            og_wait_ms=1,
            extract_ms=1,
            total_ms=4,
        )

    monkeypatch.setattr(service, "_run_instagram_fetch_with_browser", fake_run)

    settings = Settings(crawler_browser_reuse_enabled=True, crawler_recover_on_browser_crash=True)
    _, caption = _run(
        service._fetch_instagram_og_caption("https://www.instagram.com/reel/abc/", 12000, 3000, settings)
    )

    assert caption == "recovered"
    assert calls["count"] == 2
    assert runtime.shutdown_calls == 1


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_prewarm_runtime_calls_ensure_browser(monkeypatch) -> None:
    runtime = _FakeRuntime()
    monkeypatch.setattr(service, "_INSTAGRAM_RUNTIME", runtime)
    settings = Settings(crawler_browser_reuse_enabled=True)

    warmed = _run(service.prewarm_crawler_runtime(settings))

    assert warmed is True
    assert runtime.ensure_calls == 1
    assert runtime.launch_count == 1


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_INSTAGRAM_IMAGE_TESTS") != "1",
    reason="Set RUN_LIVE_INSTAGRAM_IMAGE_TESTS=1 to crawl live Instagram post images.",
)
@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
@pytest.mark.parametrize(
    "url",
    [
        "https://www.instagram.com/p/DX8pE6WEeom/?",
        "https://www.instagram.com/p/DYgvDx0gc_X/?img_index=1",
        "https://www.instagram.com/p/C6Kp79xRiGC",
        "https://www.instagram.com/p/DLmXSK3znl-",
    ],
)
def test_live_instagram_post_image_fetch_returns_images(url: str) -> None:
    settings = Settings(
        instagram_image_fetch_timeout_ms=12000,
        instagram_image_fetch_max_images=12,
        instagram_image_fetch_max_next_clicks=12,
        crawler_browser_reuse_enabled=False,
    )

    result = _run_with_subprocesses(service.fetch_instagram_post_images(url, settings))

    assert result.timed_out is False
    assert result.response_status != 429
    assert result.image_urls
