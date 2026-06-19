from __future__ import annotations

import html
import logging
import re
import time
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from urllib.parse import urlparse

import httpx

from app.core.config import Settings
from app.services.crawler.instagram_reel_parse import parse_instagram_meta_description

logger = logging.getLogger("processing.crawler.instagram_http_meta")

FACEBOOK_EXTERNAL_HIT_UA = "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)"
TWITTERBOT_UA = "Twitterbot/1.0"
DESKTOP_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

USER_AGENT_CANDIDATES: tuple[tuple[str, str], ...] = (
    ("facebookexternalhit", FACEBOOK_EXTERNAL_HIT_UA),
    ("twitterbot", TWITTERBOT_UA),
    ("desktop_chrome", DESKTOP_CHROME_UA),
)



GENERIC_INSTAGRAM_PHRASES = (
    "create an account or log in",
    "log in to instagram",
    "instagram에서 사진 및 동영상 보기",
    "see instagram photos and videos",
    "instagram photos and videos",
    "sign up to see photos",
)

_WHITESPACE_RE = re.compile(r"\s+")
_MEANINGFUL_RE = re.compile(r"[0-9A-Za-z가-힣]")


@dataclass(slots=True)
class InstagramHttpMetaResult:
    url: str
    ua_type: str
    status_code: int | None
    final_url: str | None
    title: str
    og_title: str
    og_description: str
    og_image: str
    og_url: str
    description: str
    cleaned_description: str
    html_length: int
    elapsed_ms: int
    login_gate: bool
    challenge: bool
    rate_limited: bool
    generic_instagram_page: bool
    useful: bool
    failure_reason: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @property
    def should_skip_playwright(self) -> bool:
        return self.useful or self.rate_limited or self.challenge


@dataclass(slots=True)
class InstagramHttpMetaFetchResult:
    selected: InstagramHttpMetaResult
    attempts: list[InstagramHttpMetaResult]


class _MetaTagParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title = ""
        self._in_title = False
        self.meta: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        lower_tag = tag.lower()
        if lower_tag == "title":
            self._in_title = True
        if lower_tag == "meta":
            self.meta.append({str(key).lower(): str(value or "") for key, value in attrs})

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data

    def value_for(self, name: str) -> str:
        for attrs in self.meta:
            if attrs.get("property", "").lower() == name or attrs.get("name", "").lower() == name:
                return _clean_meta_value(attrs.get("content") or "")
        return ""


async def fetch_instagram_http_meta(
    url: str,
    settings: Settings,
) -> InstagramHttpMetaFetchResult:
    attempts: list[InstagramHttpMetaResult] = []
    timeout = httpx.Timeout(max(1, settings.instagram_http_meta_timeout_seconds))
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for ua_type, user_agent in USER_AGENT_CANDIDATES:
            result = await _fetch_with_user_agent(client, url, settings, ua_type, user_agent)
            attempts.append(result)
            _log_attempt(result, fallback_to_playwright=not result.should_skip_playwright)
            if result.useful or result.rate_limited or result.challenge:
                return InstagramHttpMetaFetchResult(selected=result, attempts=attempts)

    return InstagramHttpMetaFetchResult(selected=attempts[-1], attempts=attempts)


def extract_meta_from_html(
    *,
    url: str,
    ua_type: str,
    status_code: int | None,
    final_url: str | None,
    text: str,
    elapsed_ms: int,
    error: str | None = None,
) -> InstagramHttpMetaResult:
    parser = _MetaTagParser()
    if text:
        parser.feed(text)

    title = _clean_meta_value(parser.title)
    og_title = parser.value_for("og:title")
    og_description = parser.value_for("og:description")
    og_image = parser.value_for("og:image")
    og_url = parser.value_for("og:url")
    description = parser.value_for("description")
    source_description = og_description or description
    cleaned_description = clean_instagram_meta_description(source_description)

    lower_text = text.lower()
    login_gate = _is_login_gate(final_url, lower_text)
    challenge = _is_challenge(final_url, lower_text)
    rate_limited = status_code == 429
    generic_instagram_page = _is_generic_instagram_page(
        title=title,
        og_title=og_title,
        description=source_description,
        cleaned_description=cleaned_description,
    )
    useful, failure_reason = _usefulness(
        status_code=status_code,
        cleaned_description=cleaned_description,
        login_gate=login_gate,
        challenge=challenge,
        rate_limited=rate_limited,
        generic_instagram_page=generic_instagram_page,
        error=error,
    )

    return InstagramHttpMetaResult(
        url=url,
        ua_type=ua_type,
        status_code=status_code,
        final_url=final_url,
        title=title,
        og_title=og_title,
        og_description=og_description,
        og_image=og_image,
        og_url=og_url,
        description=description,
        cleaned_description=cleaned_description,
        html_length=len(text or ""),
        elapsed_ms=elapsed_ms,
        login_gate=login_gate,
        challenge=challenge,
        rate_limited=rate_limited,
        generic_instagram_page=generic_instagram_page,
        useful=useful,
        failure_reason=failure_reason,
        error=error,
    )


def clean_instagram_meta_description(value: str) -> str:
    raw = _clean_meta_value(value)
    if not raw:
        return ""

    parsed = parse_instagram_meta_description(raw)
    if parsed:
        return _clean_caption(parsed["caption"])

    unquoted = raw.strip().strip('"').strip("'").strip()
    if unquoted.endswith("."):
        unquoted = unquoted[:-1].strip()
    return _clean_caption(unquoted)


def instagram_http_meta_metadata(result: InstagramHttpMetaResult) -> dict[str, object]:
    parsed = parse_instagram_meta_description(result.og_description or result.description)
    content_text = result.cleaned_description if result.useful else ""
    instagram_metadata: dict[str, object] = {
        "og_source": "http_meta" if result.useful and result.cleaned_description else "none",
        "ua_type": result.ua_type,
        "og_meta_count": sum(
            1
            for value in (
                result.og_title,
                result.og_description,
                result.og_image,
                result.og_url,
            )
            if value
        ),
        "og_title_present": bool(result.og_title),
        "og_description_present": bool(result.og_description),
        "og_image_present": bool(result.og_image),
        "og_url_present": bool(result.og_url),
        "login_gate": result.login_gate,
        "challenge": result.challenge,
        "rate_limited": result.rate_limited,
        "generic_instagram_page": result.generic_instagram_page,
        "useful": result.useful,
        "failure_reason": result.failure_reason,
        "fallback_to_playwright": False,
        "http_meta": {
            "title": result.title,
            "og_title": result.og_title,
            "og_description": result.og_description,
            "og_image": result.og_image,
            "og_url": result.og_url,
            "description": result.description,
            "cleaned_description": result.cleaned_description,
            "elapsed_ms": result.elapsed_ms,
            "error": result.error,
        },
    }
    metadata: dict[str, object] = {
        "extraction_source": "instagram_http_meta",
        "response_status": result.status_code,
        "response_url": result.final_url,
        "final_url": result.final_url,
        "html_len": result.html_length,
        "body_text_len": len(content_text),
        "empty_body": not bool(content_text),
        "instagram": instagram_metadata,
    }
    if parsed:
        metadata.update(
            {
                "likes": parsed["likes"],
                "comments": parsed["comments"],
                "posted_at": parsed["posted_at"],
                "likes_text": parsed["likes_text"],
                "comments_text": parsed["comments_text"],
                "posted_at_text": parsed["posted_at_text"],
            }
        )
    return metadata


def public_debug_dict(fetch_result: InstagramHttpMetaFetchResult) -> dict[str, object]:
    selected = fetch_result.selected
    return {
        "status_code": selected.status_code,
        "final_url": selected.final_url,
        "title": selected.title,
        "og_title": selected.og_title,
        "og_description": selected.og_description,
        "og_image": selected.og_image,
        "og_url": selected.og_url,
        "description": selected.description,
        "cleaned_description": selected.cleaned_description,
        "block_detection": {
            "login_gate": selected.login_gate,
            "challenge": selected.challenge,
            "rate_limited": selected.rate_limited,
            "generic_instagram_page": selected.generic_instagram_page,
        },
        "useful": selected.useful,
        "selected_ua_type": selected.ua_type,
        "failure_reason": selected.failure_reason,
        "attempts": [attempt.to_dict() for attempt in fetch_result.attempts],
    }


async def _fetch_with_user_agent(
    client: httpx.AsyncClient,
    url: str,
    settings: Settings,
    ua_type: str,
    user_agent: str,
) -> InstagramHttpMetaResult:
    started = time.monotonic()
    headers = {
        "user-agent": user_agent,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": f"{settings.instagram_locale},{settings.instagram_locale.split('-', 1)[0]};q=0.9,en-US;q=0.8,en;q=0.7",
    }
    try:
        response = await client.get(url, headers=headers)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        return extract_meta_from_html(
            url=url,
            ua_type=ua_type,
            status_code=response.status_code,
            final_url=str(response.url),
            text=response.text,
            elapsed_ms=elapsed_ms,
        )
    except (httpx.TimeoutException, httpx.NetworkError) as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        return extract_meta_from_html(
            url=url,
            ua_type=ua_type,
            status_code=None,
            final_url=None,
            text="",
            elapsed_ms=elapsed_ms,
            error=f"{exc.__class__.__name__}: {exc}",
        )


def _clean_meta_value(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", html.unescape(value or "")).strip()


def _clean_caption(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", (value or "").strip()).strip('"').strip("'").strip()


def _is_login_gate(final_url: str | None, lower_text: str) -> bool:
    path = urlparse(final_url or "").path.lower()
    return "/accounts/login" in path or "log in to instagram" in lower_text


def _is_challenge(final_url: str | None, lower_text: str) -> bool:
    path = urlparse(final_url or "").path.lower()
    return (
        "/challenge" in path
        or "/checkpoint" in path
        or "challenge_required" in lower_text
        or "checkpoint_required" in lower_text
    )


def _is_generic_instagram_page(
    *,
    title: str,
    og_title: str,
    description: str,
    cleaned_description: str,
) -> bool:
    haystack = " ".join([title, og_title, description, cleaned_description]).lower()
    if any(phrase.lower() in haystack for phrase in GENERIC_INSTAGRAM_PHRASES):
        return True
    return not cleaned_description and (title.strip().lower() in {"instagram", ""})


def _usefulness(
    *,
    status_code: int | None,
    cleaned_description: str,
    login_gate: bool,
    challenge: bool,
    rate_limited: bool,
    generic_instagram_page: bool,
    error: str | None,
) -> tuple[bool, str | None]:
    if error:
        return False, "request_failed"
    if status_code is None:
        return False, "missing_status"
    if rate_limited:
        return False, "rate_limited"
    if not 200 <= status_code < 300:
        return False, f"http_{status_code}"
    if challenge:
        return False, "challenge"
    if login_gate:
        return False, "login_gate"
    if generic_instagram_page:
        return False, "generic_instagram_page"
    if len(cleaned_description) < 20:
        return False, "description_too_short"
    if not _MEANINGFUL_RE.search(cleaned_description):
        return False, "description_not_meaningful"
    return True, None


def _log_attempt(result: InstagramHttpMetaResult, *, fallback_to_playwright: bool) -> None:
    logger.info(
        (
            "instagram http meta url=%s ua_type=%s status_code=%s final_url=%s "
            "html_length=%s og_title_present=%s og_description_present=%s "
            "og_image_present=%s login_gate=%s challenge=%s rate_limited=%s "
            "generic_instagram_page=%s useful=%s elapsed_ms=%s fallback_to_playwright=%s"
        ),
        result.url,
        result.ua_type,
        result.status_code,
        result.final_url,
        result.html_length,
        bool(result.og_title),
        bool(result.og_description),
        bool(result.og_image),
        result.login_gate,
        result.challenge,
        result.rate_limited,
        result.generic_instagram_page,
        result.useful,
        result.elapsed_ms,
        fallback_to_playwright,
    )
