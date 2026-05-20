from __future__ import annotations

from typing import Protocol

from app.services.crawler.extractors.types import ExtractedContent, LinkStats


class LinkStatsExtractor(Protocol):
    name: str

    def supports(self, url: str) -> bool: ...

    async def extract(self, content: ExtractedContent) -> LinkStats: ...
