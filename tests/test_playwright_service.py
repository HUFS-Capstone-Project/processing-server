from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

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
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


class _FakeRuntime:
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
            return object(), 17
        return object(), 0

    async def shutdown(self) -> None:
        self.shutdown_calls += 1
        self._launched = False


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_instagram_fetch_reuses_browser_launch(monkeypatch) -> None:
    runtime = _FakeRuntime()
    monkeypatch.setattr(service, "_INSTAGRAM_RUNTIME", runtime)

    async def fake_run(**_kwargs):
        return service.InstagramFetchResult(
            caption="ok",
            og_source="og:description",
            og_wait_timed_out=False,
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
