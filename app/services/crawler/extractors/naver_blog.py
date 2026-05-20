from __future__ import annotations

import logging

from app.core.config import Settings
from app.services.crawler.extractors.base import ContentExtractor
from app.services.crawler.extractors.generic_web import GenericWebContentExtractor
from app.services.crawler.extractors.types import (
    ExtractedContent,
    ExtractionMethod,
    SourceType,
)
from app.services.crawler.playwright_service import (
    NaverBlogFetchResult,
    fetch_naver_blog_content,
    safe_url_for_log,
)
from app.services.crawler.naver_blog import is_naver_blog_url

logger = logging.getLogger("processing.crawler.naver_blog")


class NaverBlogContentExtractor:
    name = "naver_blog"

    def __init__(
        self,
        settings: Settings,
        *,
        fallback_extractor: ContentExtractor | None = None,
    ) -> None:
        self._settings = settings
        self._fallback_extractor = fallback_extractor or GenericWebContentExtractor(settings)

    def supports(self, url: str) -> bool:
        return is_naver_blog_url(url)

    async def extract(self, url: str) -> ExtractedContent:
        try:
            result = await fetch_naver_blog_content(url, self._settings)
            if result.content_text.strip():
                return self._from_naver_result(url, result)
            logger.info("naver blog dedicated extraction returned empty text url=%s", safe_url_for_log(url))
            return await self._fallback(url)
        except Exception as exc:
            logger.warning(
                "naver blog dedicated extraction failed; falling back url=%s error=%s",
                safe_url_for_log(url),
                exc.__class__.__name__,
                exc_info=True,
            )
            return await self._fallback(url)

    @staticmethod
    def _from_naver_result(url: str, result: NaverBlogFetchResult) -> ExtractedContent:
        return ExtractedContent(
            source_url=url,
            source_type=SourceType.NAVER_BLOG,
            content_text=result.content_text.strip(),
            extraction_method=ExtractionMethod.NAVER_BLOG_POST_VIEW,
            raw_metadata={
                "extraction_source": result.extraction_source,
                "final_url": result.resolved_url,
                "html_len": len(result.html or ""),
                "body_text_len": len(result.content_text or ""),
                "empty_body": not bool((result.content_text or "").strip()),
                "naver_blog": {
                    "extraction_source": result.extraction_source,
                    "resolved_url": result.resolved_url,
                    "iframe_src": result.iframe_src,
                    "log_no": result.log_no,
                    "selected_selector": result.selected_selector,
                    "like_count_text": result.like_count_text,
                    "comment_count_text": result.comment_count_text,
                    "posted_at_text": result.posted_at_text,
                },
            },
            html=result.html,
        )

    async def _fallback(self, url: str) -> ExtractedContent:
        fallback = await self._fallback_extractor.extract(url)
        raw_metadata = dict(fallback.raw_metadata or {})
        raw_metadata.pop("html", None)
        raw_metadata.update(
            {
                "extraction_source": "generic_fallback",
                "final_url": fallback.source_url,
                "html_len": len(fallback.html or ""),
                "body_text_len": len(fallback.content_text or ""),
                "empty_body": not bool((fallback.content_text or "").strip()),
                "naver_blog": {
                    "extraction_source": "generic_fallback",
                    "resolved_url": fallback.source_url,
                    "iframe_src": None,
                    "log_no": None,
                    "selected_selector": None,
                    "like_count_text": None,
                    "comment_count_text": None,
                    "posted_at_text": None,
                },
                "fallback_source_type": fallback.source_type.value,
                "fallback_extraction_method": (
                    fallback.extraction_method.value if fallback.extraction_method else None
                ),
            }
        )
        return ExtractedContent(
            source_url=url,
            source_type=SourceType.NAVER_BLOG,
            content_text=fallback.content_text,
            extraction_method=ExtractionMethod.NAVER_BLOG_GENERIC_FALLBACK,
            raw_metadata=raw_metadata,
            html=fallback.html,
        )
