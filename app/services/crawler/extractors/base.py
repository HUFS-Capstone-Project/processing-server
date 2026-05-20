from __future__ import annotations

from typing import Protocol

from app.services.crawler.extractors.types import ExtractedContent


class ContentExtractor(Protocol):
    name: str

    def supports(self, url: str) -> bool: ...

    async def extract(self, url: str) -> ExtractedContent: ...
