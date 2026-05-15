from __future__ import annotations

from app.domain.business_hours import BusinessHoursParseResult
from app.services.business_hours.kakao_place import (
    BusinessHoursRow,
    parse_kakao_place_business_hours_html,
    parse_kakao_place_business_hours_rows,
)


class BusinessHoursParser:
    def parse_html(self, html_content: str) -> BusinessHoursParseResult:
        return parse_kakao_place_business_hours_html(html_content)

    def parse_rows(
        self,
        rows: list[BusinessHoursRow],
        *,
        operation_section_found: bool,
    ) -> BusinessHoursParseResult:
        return parse_kakao_place_business_hours_rows(
            rows,
            operation_section_found=operation_section_found,
        )

