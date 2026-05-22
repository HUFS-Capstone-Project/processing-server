from __future__ import annotations

from typing import Any, Protocol

from app.core.config import Settings
from app.services.crawler.extractors.types import (
    ExtractedContent,
    ExtractionMethod,
    SourceType,
)
from app.services.crawler.youtube import (
    canonical_youtube_video_url,
    extract_youtube_video_id,
    is_youtube_host,
)
from app.services.crawler.youtube_data_api import YouTubeDataApiClient, YouTubeVideoResult


class YouTubeClient(Protocol):
    async def fetch_video(self, video_id: str) -> YouTubeVideoResult: ...


class YouTubeContentExtractor:
    name = "youtube"

    def __init__(self, settings: Settings, *, client: YouTubeClient | None = None) -> None:
        self._settings = settings
        self._client = client or YouTubeDataApiClient(settings)

    def supports(self, url: str) -> bool:
        return is_youtube_host(url)

    async def extract(self, url: str) -> ExtractedContent:
        video_id = extract_youtube_video_id(url)
        canonical_url = canonical_youtube_video_url(url)
        if not video_id or not canonical_url:
            raise ValueError("Unsupported or malformed YouTube video URL")

        result = await self._client.fetch_video(video_id)
        content_text = build_youtube_content_text(
            result.video,
            result.uploader_comments,
            self._settings,
        )
        snippet = result.video.get("snippet") or {}
        statistics = result.video.get("statistics") or {}
        return ExtractedContent(
            source_url=canonical_url,
            source_type=SourceType.YOUTUBE,
            content_text=content_text,
            extraction_method=ExtractionMethod.YOUTUBE_DATA_API,
            raw_metadata={
                "youtube": {
                    "video_id": video_id,
                    "canonical_url": canonical_url,
                    "snippet": snippet,
                    "statistics": statistics,
                    "uploader_comments": result.uploader_comments,
                    "comments_error": result.comments_error,
                }
            },
            html=None,
        )


def build_youtube_content_text(
    video: dict[str, Any],
    uploader_comments: list[dict[str, Any]],
    settings: Settings,
) -> str:
    snippet = video.get("snippet") or {}
    sections: list[tuple[str, str]] = []

    title = _clean(snippet.get("title"))
    if title:
        sections.append(("[제목]", title))

    description = _truncate(_clean(snippet.get("description")), settings.youtube_description_max_chars)
    if description:
        sections.append(("[설명]", description))

    tags = snippet.get("tags") or []
    tag_text = "\n".join(_format_tag(tag) for tag in tags if _clean(tag))
    if tag_text:
        sections.append(("[태그]", tag_text))

    comments = [
        _truncate(_clean(comment.get("text")), settings.youtube_comment_max_chars)
        for comment in uploader_comments
    ]
    comment_text = "\n\n".join(comment for comment in comments if comment)
    if comment_text:
        sections.append(("[작성자 댓글]", comment_text))

    text = "\n\n".join(f"{header}\n{body}" for header, body in sections)
    return _truncate(text.strip(), settings.youtube_content_max_chars)


def _format_tag(tag: object) -> str:
    text = _clean(tag)
    if not text:
        return ""
    return text if text.startswith("#") else f"#{text}"


def _clean(value: object) -> str:
    return str(value or "").strip()


def _truncate(text: str, max_chars: int) -> str:
    limit = max(0, int(max_chars))
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit].rstrip()
