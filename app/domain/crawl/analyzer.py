from __future__ import annotations

import logging
from dataclasses import replace

from app.core.config import Settings
from app.domain.job.model import CrawlArtifact
from app.domain.url_contract import (
    crawl_url_for,
    instagram_media_type,
    is_instagram_media_url,
)
from app.services.crawler.instagram_reel_parse import parse_instagram_reel_meta
from app.services.crawler.extractors import ContentExtractorRegistry
from app.services.crawler.extractors.link_stats_registry import LinkStatsExtractorRegistry

logger = logging.getLogger("processing.crawl.analyzer")


async def crawl_and_parse(url: str, settings: Settings) -> CrawlArtifact:
    crawl_url = crawl_url_for(url)
    registry = ContentExtractorRegistry(settings)
    selected_extractor, content = await registry.extract(crawl_url)
    clean_text = (content.content_text or "").strip()

    media_type = instagram_media_type(crawl_url) if is_instagram_media_url(crawl_url) else None
    content_text = clean_text
    parsed_metadata = None

    if media_type and clean_text:
        parsed_metadata = parse_instagram_reel_meta(clean_text)
        if parsed_metadata:
            content_text = parsed_metadata.get("caption") or clean_text

    raw_metadata = dict(content.raw_metadata or {})
    stats_content = (
        replace(content, raw_metadata=parsed_metadata)
        if parsed_metadata
        else content
    )
    link_stats = await LinkStatsExtractorRegistry().extract_best_effort(stats_content)
    logger.info(
        (
            "crawl extracted crawl_url=%s selected_extractor=%s source_type=%s "
            "extraction_method=%s content_text_len=%s stats_source=%s stats_available=%s"
        ),
        crawl_url,
        selected_extractor.name,
        content.source_type.value,
        content.extraction_method.value if content.extraction_method else None,
        len(content_text),
        link_stats.stats_source.value,
        link_stats.is_available,
    )

    return CrawlArtifact(
        url=crawl_url,
        html=content.html,
        content_text=content_text,
        media_type=media_type,
        source_type=content.source_type.value,
        extraction_method=content.extraction_method.value if content.extraction_method else None,
        raw_metadata=raw_metadata,
        link_stats=link_stats,
    )
