from __future__ import annotations

import asyncio
import html
import logging
import re
from dataclasses import dataclass
from typing import Any

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from app.core.config import Settings
from app.domain.business_hours import BusinessHoursDetailStatus, BusinessHoursParseResult
from app.services.crawler.playwright_service import _browser_args

logger = logging.getLogger("processing.business_hours.kakao_place")

PRIMARY_LINE_SELECTOR = ".detail_info.info_operation .fold_detail .line_fold"
OPERATION_SCOPE_SELECTOR = ".detail_info.info_operation"
UNIT_DEFAULT_SELECTOR = ".unit_default"
FOLD_LINE_SELECTOR = ".fold_detail .line_fold"

DAY_TEXT_RE = re.compile(
    r"^(?P<day>[\uC6D4\uD654\uC218\uBAA9\uAE08\uD1A0\uC77C]|"
    r"\uD3C9\uC77C|\uC8FC\uB9D0|\uACF5\uD734\uC77C)"
    r"(?:\((?P<date>[^)]+)\))?$"
)
TIME_RANGE_RE = re.compile(
    r"^(?P<open>\d{1,2}:\d{2})\s*(?:~|-)\s*(?P<close>\d{1,2}:\d{2})$"
)
SPECIAL_HOURS_RE = re.compile(
    r"^(?P<value>\uD734\uBB34|\uD734\uBB34\uC77C|\uC815\uAE30\uD734\uBB34|24\uC2DC\uAC04|\uC5F0\uC911\uBB34\uD734)$"
)
BREAK_TIME_RE = re.compile(
    r"^(?P<open>\d{1,2}:\d{2})\s*(?:~|-)\s*(?P<close>\d{1,2}:\d{2})\s*\uBE0C\uB808\uC774\uD06C\uD0C0\uC784$"
)
LAST_ORDER_RE = re.compile(r"^(?P<time>\d{1,2}:\d{2})\s*\uB77C\uC2A4\uD2B8\uC624\uB354$")


class KakaoPlaceCrawlError(Exception):
    pass


@dataclass(slots=True)
class BusinessHoursRow:
    day_text: str
    hours_text: str
    detail_texts: list[str] | None = None


async def fetch_kakao_place_business_hours(
    place_url: str,
    settings: Settings,
) -> BusinessHoursParseResult:
    timeout_ms = max(1, settings.business_hours_crawl_timeout_seconds) * 1000
    try:
        return await asyncio.wait_for(
            _fetch_kakao_place_business_hours(place_url, timeout_ms, settings),
            timeout=max(5, settings.business_hours_crawl_timeout_seconds + 5),
        )
    except PlaywrightTimeoutError as exc:
        raise KakaoPlaceCrawlError(str(exc)) from exc
    except asyncio.TimeoutError as exc:
        raise KakaoPlaceCrawlError("Kakao place crawl hard timeout") from exc


async def _fetch_kakao_place_business_hours(
    place_url: str,
    timeout_ms: int,
    settings: Settings,
) -> BusinessHoursParseResult:
    logger.info("business hours crawl start url=%s timeout_ms=%s", place_url, timeout_ms)
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=_browser_args(settings))
            try:
                page = await browser.new_page(locale="ko-KR")
                await page.goto(place_url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    await page.wait_for_load_state("networkidle", timeout=min(3000, timeout_ms))
                except PlaywrightTimeoutError:
                    logger.info("business hours networkidle wait timeout url=%s", place_url)

                rows_payload = await page.evaluate(_DOM_ROW_EXTRACTION_JS)
            finally:
                await browser.close()
    except Exception as exc:
        if isinstance(exc, KakaoPlaceCrawlError):
            raise
        raise KakaoPlaceCrawlError(str(exc)) from exc

    rows = _rows_from_payload(rows_payload)
    return parse_kakao_place_business_hours_rows(rows, operation_section_found=bool(rows_payload))


def parse_kakao_place_business_hours_html(html_content: str) -> BusinessHoursParseResult:
    operation_html = _extract_operation_scope_html(html_content)
    if not operation_html:
        return BusinessHoursParseResult(
            status=BusinessHoursDetailStatus.NOT_FOUND,
            business_hours=None,
            raw_text=None,
        )

    line_html_blocks = _extract_line_fold_blocks(operation_html)
    if not line_html_blocks:
        return BusinessHoursParseResult(
            status=BusinessHoursDetailStatus.PARSE_FAILED,
            business_hours=None,
            raw_text=None,
            error_message="Business hours operation section has no line_fold rows.",
        )

    rows = [_row_from_line_fold_html(block) for block in line_html_blocks]
    rows = [row for row in rows if row is not None]
    return parse_kakao_place_business_hours_rows(rows, operation_section_found=True)


def parse_kakao_place_business_hours_rows(
    rows: list[BusinessHoursRow],
    *,
    operation_section_found: bool,
) -> BusinessHoursParseResult:
    if not operation_section_found:
        return BusinessHoursParseResult(
            status=BusinessHoursDetailStatus.NOT_FOUND,
            business_hours=None,
            raw_text=None,
        )

    if not rows:
        return BusinessHoursParseResult(
            status=BusinessHoursDetailStatus.PARSE_FAILED,
            business_hours=None,
            raw_text=None,
            error_message="Business hours operation section has no parseable line_fold rows.",
        )

    daily_hours = [_normalize_row(row) for row in rows]
    daily_hours = [entry for entry in daily_hours if entry is not None]
    if not daily_hours:
        return BusinessHoursParseResult(
            status=BusinessHoursDetailStatus.PARSE_FAILED,
            business_hours=None,
            raw_text=None,
            error_message="Business hours line_fold rows had no valid day/hour pairs.",
        )

    raw_lines = [f"{entry['day_text']} {entry['raw']}" for entry in daily_hours]
    business_hours = {
        "daily_hours": [
            {
                key: value
                for key, value in entry.items()
                if key in {"day", "date", "raw", "open", "close", "type", "break_time", "last_order"}
            }
            for entry in daily_hours
        ]
    }
    return BusinessHoursParseResult(
        status=BusinessHoursDetailStatus.SUCCESS,
        business_hours=business_hours,
        raw_text="\n".join(raw_lines),
    )


def _normalize_row(row: BusinessHoursRow) -> dict[str, str | None] | None:
    day_text = _compact_text(row.day_text)
    hours_text = _compact_text(row.hours_text)
    day_match = DAY_TEXT_RE.fullmatch(day_text)
    if not day_match or not hours_text:
        return None

    time_match = TIME_RANGE_RE.fullmatch(hours_text)
    if time_match:
        entry = {
            "day_text": day_text,
            "day": day_match.group("day"),
            "date": day_match.group("date"),
            "raw": hours_text,
            "open": time_match.group("open"),
            "close": time_match.group("close"),
        }
        break_time = _extract_break_time(row.detail_texts or [])
        if break_time:
            entry["break_time"] = break_time
        last_order = _extract_last_order(row.detail_texts or [])
        if last_order:
            entry["last_order"] = last_order
        return entry

    special_match = SPECIAL_HOURS_RE.fullmatch(hours_text)
    if special_match:
        return {
            "day_text": day_text,
            "day": day_match.group("day"),
            "date": day_match.group("date"),
            "raw": hours_text,
            "type": special_match.group("value"),
        }

    return None


def _extract_break_time(detail_texts: list[str]) -> dict[str, str] | None:
    for detail_text in detail_texts[1:]:
        match = BREAK_TIME_RE.fullmatch(_compact_text(detail_text))
        if not match:
            continue
        return {
            "raw": _compact_text(detail_text),
            "open": match.group("open"),
            "close": match.group("close"),
        }
    return None


def _extract_last_order(detail_texts: list[str]) -> dict[str, str] | None:
    for detail_text in detail_texts[1:]:
        match = LAST_ORDER_RE.fullmatch(_compact_text(detail_text))
        if not match:
            continue
        return {
            "raw": _compact_text(detail_text),
            "time": match.group("time"),
        }
    return None


def _rows_from_payload(payload: Any) -> list[BusinessHoursRow]:
    if not isinstance(payload, list):
        return []
    rows: list[BusinessHoursRow] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        rows.append(
            BusinessHoursRow(
                day_text=str(item.get("dayText") or ""),
                hours_text=str(item.get("hoursText") or ""),
                detail_texts=[
                    str(value or "")
                    for value in item.get("detailTexts", [])
                    if isinstance(item.get("detailTexts"), list)
                ],
            )
        )
    return rows


def _extract_operation_scope_html(html_content: str) -> str | None:
    scope = _extract_first_balanced_block(html_content, OPERATION_SCOPE_SELECTOR)
    if scope:
        return scope
    return _extract_unit_default_with_operation_label(html_content)


def _extract_unit_default_with_operation_label(html_content: str) -> str | None:
    blocks = _extract_balanced_blocks(html_content, UNIT_DEFAULT_SELECTOR)
    for block in blocks:
        text = _html_to_text(block)
        if "\uC601\uC5C5\uC815\uBCF4" in text:
            return block
    return None


def _extract_line_fold_blocks(scope_html: str) -> list[str]:
    return _extract_balanced_blocks(scope_html, ".line_fold")


def _row_from_line_fold_html(line_html: str) -> BusinessHoursRow | None:
    day_text = _extract_first_class_text(line_html, "tit_fold")
    detail_texts = _extract_class_texts(line_html, "txt_detail")
    if day_text is None or not detail_texts:
        return None
    return BusinessHoursRow(
        day_text=day_text,
        hours_text=detail_texts[0],
        detail_texts=detail_texts,
    )


def _extract_first_class_text(html_content: str, class_name: str) -> str | None:
    texts = _extract_class_texts(html_content, class_name)
    return texts[0] if texts else None


def _extract_class_texts(html_content: str, class_name: str) -> list[str]:
    pattern = re.compile(
        rf"<(?P<tag>[a-zA-Z0-9]+)(?=[^>]*class=[\"'][^\"']*\b{re.escape(class_name)}\b[^\"']*[\"'])[^>]*>(?P<body>.*?)</(?P=tag)>",
        re.IGNORECASE | re.DOTALL,
    )
    return [
        text
        for text in (_html_to_text(match.group("body")) for match in pattern.finditer(html_content))
        if text
    ]


def _extract_first_balanced_block(html_content: str, selector: str) -> str | None:
    blocks = _extract_balanced_blocks(html_content, selector)
    return blocks[0] if blocks else None


def _extract_balanced_blocks(html_content: str, selector: str) -> list[str]:
    class_names = [part for part in selector.split(".") if part]
    if not class_names:
        return []

    start_pattern = re.compile(
        r"<(?P<tag>[a-zA-Z0-9]+)(?=[^>]*class=[\"'](?P<class>[^\"']*)[\"'])[^>]*>",
        re.IGNORECASE,
    )
    blocks: list[str] = []
    pos = 0
    while True:
        match = start_pattern.search(html_content, pos)
        if not match:
            break
        classes = set((match.group("class") or "").split())
        if not set(class_names).issubset(classes):
            pos = match.end()
            continue
        block = _balanced_element_html(html_content, match.start(), match.group("tag"))
        if block:
            blocks.append(block)
            pos = match.start() + len(block)
        else:
            pos = match.end()
    return blocks


def _balanced_element_html(html_content: str, start: int, tag: str) -> str | None:
    pattern = re.compile(rf"</?{re.escape(tag)}\b[^>]*>", re.IGNORECASE)
    depth = 0
    for match in pattern.finditer(html_content, start):
        token = match.group(0)
        is_close = token.startswith("</")
        is_self_closing = token.endswith("/>")
        if not is_close:
            depth += 1
            if is_self_closing:
                depth -= 1
        else:
            depth -= 1
        if depth == 0:
            return html_content[start : match.end()]
    return None


def _html_to_text(html_content: str) -> str:
    content = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html_content)
    content = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", content)
    content = re.sub(r"<[^>]+>", " ", content)
    return _compact_text(html.unescape(content))


def _compact_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


_DOM_ROW_EXTRACTION_JS = f"""
() => {{
  const text = (el) => (el && el.textContent ? el.textContent.replace(/\\s+/g, ' ').trim() : '');
  const rowsFromScope = (scope) => {{
    if (!scope) return [];
    return Array.from(scope.querySelectorAll('{FOLD_LINE_SELECTOR}')).map((row) => ({{
      dayText: text(row.querySelector('.tit_fold')),
      hoursText: text(row.querySelector('.detail_fold .txt_detail')),
      detailTexts: Array.from(row.querySelectorAll('.detail_fold .txt_detail')).map(text).filter(Boolean)
    }})).filter((row) => row.dayText || row.hoursText);
  }};

  let rows = rowsFromScope(document.querySelector('{OPERATION_SCOPE_SELECTOR}'));
  if (rows.length) return rows;

  const units = Array.from(document.querySelectorAll('{UNIT_DEFAULT_SELECTOR}'));
  const operationUnit = units.find((unit) => text(unit).includes('영업정보'));
  rows = rowsFromScope(operationUnit);
  if (rows.length) return rows;

  const operationScope = document.querySelector('{OPERATION_SCOPE_SELECTOR}');
  if (!operationScope) return null;
  return [];
}}
"""
