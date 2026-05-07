from app.domain.business_hours import BusinessHoursFetchStatus
from app.services.business_hours.parser import BusinessHoursParser


def test_business_hours_parser_facade_delegates_html_parser() -> None:
    html = """
    <div class="detail_info info_operation">
      <div class="fold_detail">
        <div class="line_fold">
          <span class="tit_fold">월</span>
          <div class="detail_fold"><span class="txt_detail">09:00 ~ 18:00</span></div>
        </div>
      </div>
    </div>
    """

    result = BusinessHoursParser().parse_html(html)

    assert result.status == BusinessHoursFetchStatus.SUCCEEDED
    assert result.business_hours == {
        "daily_hours": [
            {"day": "월", "date": None, "raw": "09:00 ~ 18:00", "open": "09:00", "close": "18:00"}
        ]
    }

