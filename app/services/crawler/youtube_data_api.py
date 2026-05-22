from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import Settings

logger = logging.getLogger("processing.crawler.youtube")


class YouTubeError(Exception):
    pass


class YouTubeRetryableError(YouTubeError):
    pass


class YouTubeNonRetryableError(YouTubeError):
    pass


@dataclass(slots=True)
class YouTubeCommentsResult:
    uploader_comments: list[dict[str, Any]]
    error: dict[str, Any] | None = None


@dataclass(slots=True)
class YouTubeVideoResult:
    video: dict[str, Any]
    uploader_comments: list[dict[str, Any]]
    comments_error: dict[str, Any] | None = None


class YouTubeDataApiClient:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._settings = settings
        self._transport = transport
        self._base_url = "https://www.googleapis.com/youtube/v3"

    async def fetch_video(self, video_id: str) -> YouTubeVideoResult:
        if not self._settings.youtube_api_key.strip():
            raise YouTubeNonRetryableError("YOUTUBE_API_KEY is empty")

        timeout = httpx.Timeout(max(1, self._settings.youtube_http_timeout_seconds))
        async with httpx.AsyncClient(timeout=timeout, transport=self._transport) as client:
            video = await self._fetch_video_metadata(client, video_id)
            comments = await self._fetch_uploader_comments_best_effort(client, video_id, video)

        return YouTubeVideoResult(
            video=video,
            uploader_comments=comments.uploader_comments,
            comments_error=comments.error,
        )

    async def _fetch_video_metadata(
        self,
        client: httpx.AsyncClient,
        video_id: str,
    ) -> dict[str, Any]:
        try:
            response = await client.get(
                f"{self._base_url}/videos",
                params={
                    "key": self._settings.youtube_api_key,
                    "part": "snippet,statistics",
                    "id": video_id,
                },
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise YouTubeRetryableError(str(exc) or exc.__class__.__name__) from exc

        if response.status_code >= 400:
            reason = _youtube_error_reason(response)
            if _is_retryable_youtube_error(response.status_code, reason):
                raise YouTubeRetryableError(
                    f"YouTube videos.list temporary failure ({response.status_code}, {reason})"
                )
            raise YouTubeNonRetryableError(
                f"YouTube videos.list failed ({response.status_code}, {reason})"
            )

        payload = response.json()
        items = payload.get("items") or []
        if not items:
            raise YouTubeNonRetryableError("YouTube video not found or unavailable")
        if not isinstance(items[0], dict):
            raise YouTubeNonRetryableError("YouTube videos.list returned invalid item")
        return items[0]

    async def _fetch_uploader_comments_best_effort(
        self,
        client: httpx.AsyncClient,
        video_id: str,
        video: dict[str, Any],
    ) -> YouTubeCommentsResult:
        if not self._settings.youtube_comments_enabled:
            return YouTubeCommentsResult(uploader_comments=[])
        if self._settings.youtube_comments_max_pages < 1:
            return YouTubeCommentsResult(uploader_comments=[])

        try:
            response = await client.get(
                f"{self._base_url}/commentThreads",
                params={
                    "key": self._settings.youtube_api_key,
                    "part": "snippet",
                    "videoId": video_id,
                    "order": "relevance",
                    "textFormat": "plainText",
                    "maxResults": max(1, min(100, self._settings.youtube_comments_max_results)),
                },
            )
        except httpx.TimeoutException as exc:
            return YouTubeCommentsResult(
                uploader_comments=[],
                error=_comment_error("timeout", exc.__class__.__name__),
            )
        except httpx.NetworkError as exc:
            return YouTubeCommentsResult(
                uploader_comments=[],
                error=_comment_error("network", exc.__class__.__name__),
            )

        if response.status_code >= 400:
            reason = _youtube_error_reason(response)
            error = _comment_error(reason, f"HTTP {response.status_code}")
            if reason == "quotaExceeded":
                logger.warning("youtube comments quota exceeded video_id=%s", video_id)
            else:
                logger.info(
                    "youtube comments unavailable video_id=%s status=%s reason=%s",
                    video_id,
                    response.status_code,
                    reason,
                )
            return YouTubeCommentsResult(uploader_comments=[], error=error)

        payload = response.json()
        comments = _uploader_comments(payload.get("items") or [], video)
        return YouTubeCommentsResult(uploader_comments=comments)


def _uploader_comments(items: list[Any], video: dict[str, Any]) -> list[dict[str, Any]]:
    channel_id = ((video.get("snippet") or {}).get("channelId") or "").strip()
    if not channel_id:
        return []

    comments: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        snippet = ((item.get("snippet") or {}).get("topLevelComment") or {}).get("snippet") or {}
        author_channel_id = (snippet.get("authorChannelId") or {}).get("value")
        if author_channel_id != channel_id:
            continue
        comments.append(
            {
                "text": snippet.get("textDisplay") or snippet.get("textOriginal") or "",
                "author_display_name": snippet.get("authorDisplayName"),
                "author_channel_id": author_channel_id,
                "published_at": snippet.get("publishedAt"),
                "updated_at": snippet.get("updatedAt"),
            }
        )
    return comments


def _youtube_error_reason(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return "unknown"
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return "unknown"
    errors = error.get("errors")
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict) and first.get("reason"):
            return str(first["reason"])
    if error.get("status"):
        return str(error["status"])
    if error.get("message"):
        return str(error["message"])
    return "unknown"


def _is_retryable_youtube_error(status_code: int, reason: str) -> bool:
    if status_code == 429 or status_code >= 500:
        return True
    return reason in {"quotaExceeded", "rateLimitExceeded", "backendError"}


def _comment_error(reason: str, message: str) -> dict[str, Any]:
    return {
        "reason": reason,
        "message": message,
    }
