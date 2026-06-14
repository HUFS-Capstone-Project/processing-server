from __future__ import annotations

import asyncio

import pytest

from app.core.config import Settings
from app.services.crawler.instagram_http_meta import (
    InstagramHttpMetaResult,
    extract_meta_from_html,
    fetch_instagram_http_meta,
)


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        coro.close()
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _result(
    *,
    ua_type: str = "facebookexternalhit",
    status_code: int | None = 200,
    final_url: str = "https://www.instagram.com/reel/abc/",
    cleaned_description: str = "Seoul cafe near Hongdae",
    useful: bool = True,
    rate_limited: bool = False,
    challenge: bool = False,
    login_gate: bool = False,
    generic_instagram_page: bool = False,
) -> InstagramHttpMetaResult:
    return InstagramHttpMetaResult(
        url="https://www.instagram.com/reel/abc/",
        ua_type=ua_type,
        status_code=status_code,
        final_url=final_url,
        title="",
        og_title="title",
        og_description=cleaned_description,
        og_image="https://example.com/image.jpg",
        og_url="https://www.instagram.com/reel/abc/",
        description=cleaned_description,
        cleaned_description=cleaned_description,
        html_length=100,
        elapsed_ms=1,
        login_gate=login_gate,
        challenge=challenge,
        rate_limited=rate_limited,
        generic_instagram_page=generic_instagram_page,
        useful=useful,
        failure_reason=None if useful else "failed",
    )


def test_extract_meta_from_html_parses_og_and_title() -> None:
    html = """
    <html>
      <head>
        <title>Instagram</title>
        <meta property="og:title" content="Cafe on Instagram">
        <meta property="og:description" content='6,408 likes, 93 comments - limeunzzo on April 17, 2026: "Seoul cafe address near Hongdae station".'>
        <meta property="og:image" content="https://example.com/a.jpg">
        <meta property="og:url" content="https://www.instagram.com/reel/abc/">
        <meta name="description" content="fallback description">
      </head>
    </html>
    """

    result = extract_meta_from_html(
        url="https://www.instagram.com/reel/abc/",
        ua_type="facebookexternalhit",
        status_code=200,
        final_url="https://www.instagram.com/reel/abc/",
        text=html,
        elapsed_ms=12,
    )

    assert result.title == "Instagram"
    assert result.og_title == "Cafe on Instagram"
    assert result.og_image == "https://example.com/a.jpg"
    assert result.og_url == "https://www.instagram.com/reel/abc/"
    assert result.cleaned_description == "Seoul cafe address near Hongdae station"
    assert result.useful is True


def test_fetch_stops_after_first_useful_user_agent(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_fetch(client, url, settings, ua_type, user_agent):
        calls.append(ua_type)
        return _result(ua_type=ua_type)

    monkeypatch.setattr(
        "app.services.crawler.instagram_http_meta._fetch_with_user_agent",
        fake_fetch,
    )

    result = _run(fetch_instagram_http_meta("https://www.instagram.com/reel/abc/", Settings()))

    assert calls == ["facebookexternalhit"]
    assert result.selected.useful is True


def test_fetch_tries_twitterbot_after_first_nonuseful_user_agent(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_fetch(client, url, settings, ua_type, user_agent):
        calls.append(ua_type)
        if ua_type == "facebookexternalhit":
            return _result(ua_type=ua_type, useful=False, cleaned_description="", generic_instagram_page=True)
        return _result(ua_type=ua_type, cleaned_description="Twitterbot found Seoul cafe")

    monkeypatch.setattr(
        "app.services.crawler.instagram_http_meta._fetch_with_user_agent",
        fake_fetch,
    )

    result = _run(fetch_instagram_http_meta("https://www.instagram.com/reel/abc/", Settings()))

    assert calls == ["facebookexternalhit", "twitterbot"]
    assert result.selected.ua_type == "twitterbot"
    assert result.selected.useful is True


def test_fetch_stops_on_rate_limit_without_more_user_agents(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_fetch(client, url, settings, ua_type, user_agent):
        calls.append(ua_type)
        return _result(
            ua_type=ua_type,
            status_code=429,
            useful=False,
            cleaned_description="",
            rate_limited=True,
        )

    monkeypatch.setattr(
        "app.services.crawler.instagram_http_meta._fetch_with_user_agent",
        fake_fetch,
    )

    result = _run(fetch_instagram_http_meta("https://www.instagram.com/reel/abc/", Settings()))

    assert calls == ["facebookexternalhit"]
    assert result.selected.rate_limited is True


def test_extract_meta_marks_challenge_not_useful() -> None:
    result = extract_meta_from_html(
        url="https://www.instagram.com/reel/abc/",
        ua_type="facebookexternalhit",
        status_code=200,
        final_url="https://www.instagram.com/challenge/",
        text="<html><head><meta property='og:description' content='Seoul cafe address'></head></html>",
        elapsed_ms=1,
    )

    assert result.challenge is True
    assert result.useful is False
    assert result.failure_reason == "challenge"
