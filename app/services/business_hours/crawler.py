from __future__ import annotations

from app.core.config import Settings
from app.domain.business_hours import BusinessHoursParseResult
from app.services.business_hours.kakao_place import fetch_kakao_place_business_hours


class KakaoPlaceCrawler:
    async def fetch_business_hours(
        self,
        place_url: str,
        settings: Settings,
    ) -> BusinessHoursParseResult:
        return await fetch_kakao_place_business_hours(place_url, settings)

