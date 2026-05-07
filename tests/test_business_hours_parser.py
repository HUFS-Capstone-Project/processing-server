from __future__ import annotations

import asyncio
from uuid import uuid4

from app.domain.business_hours import BusinessHoursFetchStatus
from app.services.business_hours import parse_kakao_place_business_hours_html
from app.services.business_hours import kakao_place


FOLD_DETAIL_HTML = """
<div class="section_comm section_defaultinfo">
  <div class="unit_default">
    <h5 class="tit_info"><span class="ico_mapdesc ico_runstate">영업정보</span></h5>
    <div class="detail_info info_operation">
      <div class="row_detail">
        <span class="tit_detail emph_point">영업 마감</span>
        <span class="txt_detail add_mdot">내일 09:00 오픈</span>
        <div class="fold_detail">
          <div class="line_fold"><span class="tit_fold emph_point3">수(5/6)</span><div class="detail_fold"><span class="txt_detail">09:00 ~ 21:00</span></div></div>
          <div class="line_fold"><span class="tit_fold">목(5/7)</span><div class="detail_fold"><span class="txt_detail">09:00 ~ 21:00</span></div></div>
          <div class="line_fold"><span class="tit_fold">금(5/8)</span><div class="detail_fold"><span class="txt_detail">09:00 ~ 21:00</span></div></div>
          <div class="line_fold"><span class="tit_fold">토(5/9)</span><div class="detail_fold"><span class="txt_detail">09:00 ~ 21:00</span></div></div>
          <div class="line_fold"><span class="tit_fold">일(5/10)</span><div class="detail_fold"><span class="txt_detail">09:00 ~ 21:00</span></div></div>
          <div class="line_fold"><span class="tit_fold">월(5/11)</span><div class="detail_fold"><span class="txt_detail">09:00 ~ 21:00</span></div></div>
          <div class="line_fold"><span class="tit_fold">화(5/12)</span><div class="detail_fold"><span class="txt_detail">09:00 ~ 21:00</span></div></div>
        </div>
      </div>
    </div>
  </div>
  <div class="unit_default">
    <h5 class="tit_info"><span class="ico_mapdesc ico_address">주소</span></h5>
    <div class="detail_info">
      <span class="txt_detail">경기 구리시 건원대로 85 1층</span>
      <span class="txt_detail add_mdot">도보 3분</span>
    </div>
  </div>
  <div class="unit_default">
    <h5 class="tit_info"><span class="ico_mapdesc ico_call2">전화</span></h5>
    <span class="txt_detail">031-553-8163</span>
  </div>
</div>
<div class="review">
  <p>방문 후기를 남겨주세요!</p>
  <p>인창동 오래된 추어탕집 춘향골추어탕 인창점 영업 시간 매일 9:00-21:00 리뷰입니다.</p>
</div>
"""


def test_kakao_place_parser_stores_only_7_fold_detail_rows() -> None:
    result = parse_kakao_place_business_hours_html(FOLD_DETAIL_HTML)

    assert result.status == BusinessHoursFetchStatus.SUCCEEDED
    assert result.business_hours == {
        "daily_hours": [
            {"day": "수", "date": "5/6", "raw": "09:00 ~ 21:00", "open": "09:00", "close": "21:00"},
            {"day": "목", "date": "5/7", "raw": "09:00 ~ 21:00", "open": "09:00", "close": "21:00"},
            {"day": "금", "date": "5/8", "raw": "09:00 ~ 21:00", "open": "09:00", "close": "21:00"},
            {"day": "토", "date": "5/9", "raw": "09:00 ~ 21:00", "open": "09:00", "close": "21:00"},
            {"day": "일", "date": "5/10", "raw": "09:00 ~ 21:00", "open": "09:00", "close": "21:00"},
            {"day": "월", "date": "5/11", "raw": "09:00 ~ 21:00", "open": "09:00", "close": "21:00"},
            {"day": "화", "date": "5/12", "raw": "09:00 ~ 21:00", "open": "09:00", "close": "21:00"},
        ]
    }
    assert result.raw_text == "\n".join(
        [
            "수(5/6) 09:00 ~ 21:00",
            "목(5/7) 09:00 ~ 21:00",
            "금(5/8) 09:00 ~ 21:00",
            "토(5/9) 09:00 ~ 21:00",
            "일(5/10) 09:00 ~ 21:00",
            "월(5/11) 09:00 ~ 21:00",
            "화(5/12) 09:00 ~ 21:00",
        ]
    )


def test_kakao_place_parser_ignores_review_global_time_pattern() -> None:
    result = parse_kakao_place_business_hours_html(FOLD_DETAIL_HTML)

    assert result.business_hours is not None
    assert len(result.business_hours["daily_hours"]) == 7
    assert all("매일 9:00-21:00" not in item["raw"] for item in result.business_hours["daily_hours"])


def test_kakao_place_parser_raw_excludes_non_hours_text() -> None:
    result = parse_kakao_place_business_hours_html(FOLD_DETAIL_HTML)

    assert result.raw_text is not None
    forbidden = [
        "방문 후기를 남겨주세요",
        "경기 구리시",
        "031-553-8163",
        "도보 3분",
        "인창동 오래된 추어탕집",
        "영업 마감",
        "내일 09:00 오픈",
    ]
    for text in forbidden:
        assert text not in result.raw_text


def test_kakao_place_parser_returns_not_found_without_operation_section() -> None:
    html = """
    <div class="section_comm section_defaultinfo">
      <div class="unit_default">
        <h5 class="tit_info"><span>주소</span></h5>
        <span class="txt_detail">경기 구리시 건원대로 85</span>
      </div>
    </div>
    """

    result = parse_kakao_place_business_hours_html(html)

    assert result.status == BusinessHoursFetchStatus.NOT_FOUND
    assert result.business_hours is None
    assert result.raw_text is None


def test_kakao_place_parser_returns_parse_failed_without_line_fold_rows() -> None:
    html = """
    <div class="detail_info info_operation">
      <div class="row_detail">
        <span class="tit_detail emph_point">영업 마감</span>
        <span class="txt_detail add_mdot">내일 09:00 오픈</span>
      </div>
    </div>
    """

    result = parse_kakao_place_business_hours_html(html)

    assert result.status == BusinessHoursFetchStatus.FAILED
    assert result.business_hours is None
    assert result.raw_text is None


def test_kakao_place_parser_returns_parse_failed_for_invalid_pairs() -> None:
    html = """
    <div class="detail_info info_operation">
      <div class="fold_detail">
        <div class="line_fold"><span class="tit_fold">리뷰</span><div class="detail_fold"><span class="txt_detail">09:00 ~ 21:00</span></div></div>
        <div class="line_fold"><span class="tit_fold">월(5/11)</span><div class="detail_fold"><span class="txt_detail">도보 3분</span></div></div>
      </div>
    </div>
    """

    result = parse_kakao_place_business_hours_html(html)

    assert result.status == BusinessHoursFetchStatus.FAILED
    assert result.business_hours is None
    assert result.raw_text is None


def test_kakao_place_parser_supports_special_hour_values() -> None:
    html = """
    <div class="detail_info info_operation">
      <div class="fold_detail">
        <div class="line_fold"><span class="tit_fold">월</span><div class="detail_fold"><span class="txt_detail">24시간</span></div></div>
        <div class="line_fold"><span class="tit_fold">화</span><div class="detail_fold"><span class="txt_detail">휴무</span></div></div>
      </div>
    </div>
    """

    result = parse_kakao_place_business_hours_html(html)

    assert result.status == BusinessHoursFetchStatus.SUCCEEDED
    assert result.business_hours == {
        "daily_hours": [
            {"day": "월", "date": None, "raw": "24시간", "type": "24시간"},
            {"day": "화", "date": None, "raw": "휴무", "type": "휴무"},
        ]
    }
    assert result.raw_text == "월 24시간\n화 휴무"


def test_kakao_place_parser_extracts_break_time_from_second_detail_text() -> None:
    html = """
    <div class="detail_info info_operation">
      <div class="fold_detail">
        <div class="line_fold">
          <span class="tit_fold emph_point3">목(5/7)</span>
          <div class="detail_fold">
            <span class="txt_detail">11:30 ~ 21:30</span>
            <span class="txt_detail">15:00 ~ 17:00 브레이크타임</span>
          </div>
        </div>
        <div class="line_fold">
          <span class="tit_fold">토(5/9)</span>
          <div class="detail_fold">
            <span class="txt_detail">11:30 ~ 21:30</span>
            <span class="txt_detail">16:00 ~ 17:00 브레이크타임</span>
          </div>
        </div>
      </div>
    </div>
    """

    result = parse_kakao_place_business_hours_html(html)

    assert result.status == BusinessHoursFetchStatus.SUCCEEDED
    assert result.business_hours == {
        "daily_hours": [
            {
                "day": "목",
                "date": "5/7",
                "raw": "11:30 ~ 21:30",
                "open": "11:30",
                "close": "21:30",
                "break_time": {
                    "raw": "15:00 ~ 17:00 브레이크타임",
                    "open": "15:00",
                    "close": "17:00",
                },
            },
            {
                "day": "토",
                "date": "5/9",
                "raw": "11:30 ~ 21:30",
                "open": "11:30",
                "close": "21:30",
                "break_time": {
                    "raw": "16:00 ~ 17:00 브레이크타임",
                    "open": "16:00",
                    "close": "17:00",
                },
            },
        ]
    }
    assert result.raw_text == "목(5/7) 11:30 ~ 21:30\n토(5/9) 11:30 ~ 21:30"


def test_kakao_place_parser_includes_closed_day_rows() -> None:
    html = """
    <div class="detail_info info_operation">
      <div class="fold_detail">
        <div class="line_fold"><span class="tit_fold emph_point3">목(5/7)</span><div class="detail_fold"><span class="txt_detail">10:30 ~ 21:30</span></div></div>
        <div class="line_fold bar_top"><span class="tit_fold">일(5/10)</span><div class="detail_fold"><span class="txt_detail emph_point">휴무일</span></div></div>
        <div class="line_fold bar_top"><span class="tit_fold">월(5/11)</span><div class="detail_fold"><span class="txt_detail">10:30 ~ 21:30</span></div></div>
      </div>
    </div>
    """

    result = parse_kakao_place_business_hours_html(html)

    assert result.status == BusinessHoursFetchStatus.SUCCEEDED
    assert result.business_hours == {
        "daily_hours": [
            {"day": "목", "date": "5/7", "raw": "10:30 ~ 21:30", "open": "10:30", "close": "21:30"},
            {"day": "일", "date": "5/10", "raw": "휴무일", "type": "휴무일"},
            {"day": "월", "date": "5/11", "raw": "10:30 ~ 21:30", "open": "10:30", "close": "21:30"},
        ]
    }
    assert result.raw_text == "목(5/7) 10:30 ~ 21:30\n일(5/10) 휴무일\n월(5/11) 10:30 ~ 21:30"


def test_kakao_place_parser_extracts_last_order_from_second_detail_text() -> None:
    html = """
    <div class="detail_info info_operation">
      <div class="fold_detail">
        <div class="line_fold">
          <span class="tit_fold emph_point3">목(5/7)</span>
          <div class="detail_fold">
            <span class="txt_detail">09:00 ~ 23:30</span>
            <span class="txt_detail">22:50 라스트오더</span>
          </div>
        </div>
      </div>
    </div>
    """

    result = parse_kakao_place_business_hours_html(html)

    assert result.status == BusinessHoursFetchStatus.SUCCEEDED
    assert result.business_hours == {
        "daily_hours": [
            {
                "day": "목",
                "date": "5/7",
                "raw": "09:00 ~ 23:30",
                "open": "09:00",
                "close": "23:30",
                "last_order": {
                    "raw": "22:50 라스트오더",
                    "time": "22:50",
                },
            }
        ]
    }
    assert result.raw_text == "목(5/7) 09:00 ~ 23:30"


def _run(coro):
    return asyncio.run(coro)


class FakePage:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    async def goto(self, url, wait_until, timeout):
        self.calls.append(("goto", url, wait_until, timeout))

    async def wait_for_load_state(self, state, timeout):
        self.calls.append(("wait_for_load_state", state, timeout))

    async def wait_for_selector(self, selector, timeout):
        self.calls.append(("wait_for_selector", selector, timeout))
        raise kakao_place.PlaywrightTimeoutError("selector timeout")

    async def wait_for_timeout(self, timeout):
        self.calls.append(("wait_for_timeout", timeout))

    async def evaluate(self, script):
        self.calls.append(("evaluate", script))
        if self.payloads:
            return self.payloads.pop(0)
        return None


class FakeBrowser:
    def __init__(self, page):
        self.page = page
        self.closed = False

    async def new_page(self, locale):
        return self.page

    async def close(self):
        self.closed = True


class FakeChromium:
    def __init__(self, browser):
        self.browser = browser

    async def launch(self, headless, args):
        return self.browser


class FakePlaywright:
    def __init__(self, browser):
        self.chromium = FakeChromium(browser)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None


def test_kakao_place_crawler_exits_early_without_networkidle(monkeypatch) -> None:
    payload = [
        {
            "dayText": "월",
            "hoursText": "09:00 ~ 21:00",
            "detailTexts": ["09:00 ~ 21:00"],
        }
    ]
    page = FakePage([payload])
    browser = FakeBrowser(page)
    monkeypatch.setattr(kakao_place, "async_playwright", lambda: FakePlaywright(browser))

    result = _run(
        kakao_place.fetch_kakao_place_business_hours(
            f"https://place.map.kakao.com/{uuid4().int}",
            kakao_place.Settings(
                business_hours_crawl_networkidle_enabled=False,
                business_hours_crawl_selector_wait_timeout_ms=10,
                business_hours_crawl_fallback_wait_timeout_ms=10,
            ),
        )
    )

    assert result.status == BusinessHoursFetchStatus.SUCCEEDED
    assert not any(call[0] == "wait_for_load_state" for call in page.calls)
    assert not any(call[0] == "wait_for_selector" for call in page.calls)
    assert not any(call[0] == "wait_for_timeout" for call in page.calls)


def test_kakao_place_crawler_fallbacks_then_returns_not_found(monkeypatch) -> None:
    page = FakePage([None, None, None])
    browser = FakeBrowser(page)
    monkeypatch.setattr(kakao_place, "async_playwright", lambda: FakePlaywright(browser))

    result = _run(
        kakao_place.fetch_kakao_place_business_hours(
            f"https://place.map.kakao.com/{uuid4().int}",
            kakao_place.Settings(
                business_hours_crawl_selector_wait_timeout_ms=10,
                business_hours_crawl_fallback_wait_timeout_ms=20,
            ),
        )
    )

    assert result.status == BusinessHoursFetchStatus.NOT_FOUND
    assert ("wait_for_timeout", 20) in page.calls


def test_kakao_place_crawler_returns_failed_when_operation_has_no_rows(monkeypatch) -> None:
    page = FakePage([None, []])
    browser = FakeBrowser(page)
    monkeypatch.setattr(kakao_place, "async_playwright", lambda: FakePlaywright(browser))

    result = _run(
        kakao_place.fetch_kakao_place_business_hours(
            f"https://place.map.kakao.com/{uuid4().int}",
            kakao_place.Settings(
                business_hours_crawl_selector_wait_timeout_ms=10,
                business_hours_crawl_fallback_wait_timeout_ms=20,
            ),
        )
    )

    assert result.status == BusinessHoursFetchStatus.FAILED
    assert not any(call == ("wait_for_timeout", 20) for call in page.calls)
