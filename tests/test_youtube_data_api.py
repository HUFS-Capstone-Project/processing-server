from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from app.core.config import Settings
from app.services.crawler.youtube_data_api import (
    YouTubeDataApiClient,
    YouTubeNonRetryableError,
    YouTubeRetryableError,
)


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        coro.close()
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


def _settings() -> Settings:
    return Settings(youtube_api_key="test-key", youtube_comments_max_results=20)


def _video_payload() -> dict[str, Any]:
    return {
        "items": [
            {
                "snippet": {
                    "title": "Title",
                    "description": "Description",
                    "channelId": "channel-1",
                    "channelTitle": "Channel",
                    "publishedAt": "2026-05-01T00:00:00Z",
                },
                "statistics": {
                    "viewCount": "1000",
                    "likeCount": "12",
                    "commentCount": "3",
                },
            }
        ]
    }


def _comment_payload() -> dict[str, Any]:
    return {
        "items": [
            {
                "snippet": {
                    "topLevelComment": {
                        "snippet": {
                            "textDisplay": "Uploader address comment",
                            "authorChannelId": {"value": "channel-1"},
                            "authorDisplayName": "Channel",
                            "publishedAt": "2026-05-02T00:00:00Z",
                        }
                    }
                }
            },
            {
                "snippet": {
                    "topLevelComment": {
                        "snippet": {
                            "textDisplay": "Viewer comment",
                            "authorChannelId": {"value": "viewer-1"},
                        }
                    }
                }
            },
        ]
    }


def _youtube_error(reason: str) -> dict[str, Any]:
    return {"error": {"errors": [{"reason": reason}], "message": reason}}


def test_youtube_client_fetches_video_and_filters_uploader_comments() -> None:
    seen_paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        if request.url.path.endswith("/videos"):
            assert request.url.params["part"] == "snippet,statistics"
            assert request.url.params["id"] == "ZJMi3m8spJA"
            return httpx.Response(200, json=_video_payload())
        if request.url.path.endswith("/commentThreads"):
            assert request.url.params["order"] == "relevance"
            assert request.url.params["textFormat"] == "plainText"
            assert request.url.params["maxResults"] == "20"
            return httpx.Response(200, json=_comment_payload())
        return httpx.Response(404)

    client = YouTubeDataApiClient(_settings(), transport=httpx.MockTransport(handler))

    result = _run(client.fetch_video("ZJMi3m8spJA"))

    assert seen_paths == ["/youtube/v3/videos", "/youtube/v3/commentThreads"]
    assert result.video["statistics"]["likeCount"] == "12"
    assert result.uploader_comments == [
        {
            "text": "Uploader address comment",
            "author_display_name": "Channel",
            "author_channel_id": "channel-1",
            "published_at": "2026-05-02T00:00:00Z",
            "updated_at": None,
        }
    ]
    assert result.comments_error is None


def test_youtube_client_videos_empty_is_non_retryable() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": []})

    client = YouTubeDataApiClient(_settings(), transport=httpx.MockTransport(handler))

    with pytest.raises(YouTubeNonRetryableError, match="not found"):
        _run(client.fetch_video("ZJMi3m8spJA"))


def test_youtube_client_videos_quota_is_retryable() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json=_youtube_error("quotaExceeded"))

    client = YouTubeDataApiClient(_settings(), transport=httpx.MockTransport(handler))

    with pytest.raises(YouTubeRetryableError, match="quotaExceeded"):
        _run(client.fetch_video("ZJMi3m8spJA"))


def test_youtube_client_videos_timeout_is_retryable() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout")

    client = YouTubeDataApiClient(_settings(), transport=httpx.MockTransport(handler))

    with pytest.raises(YouTubeRetryableError, match="timeout"):
        _run(client.fetch_video("ZJMi3m8spJA"))


@pytest.mark.parametrize(
    ("status_code", "reason", "expected_reason"),
    [
        (403, "commentsDisabled", "commentsDisabled"),
        (403, "quotaExceeded", "quotaExceeded"),
        (500, "backendError", "backendError"),
    ],
)
def test_youtube_client_comment_errors_are_best_effort(
    status_code: int,
    reason: str,
    expected_reason: str,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/videos"):
            return httpx.Response(200, json=_video_payload())
        return httpx.Response(status_code, json=_youtube_error(reason))

    client = YouTubeDataApiClient(_settings(), transport=httpx.MockTransport(handler))

    result = _run(client.fetch_video("ZJMi3m8spJA"))

    assert result.uploader_comments == []
    assert result.comments_error == {
        "reason": expected_reason,
        "message": f"HTTP {status_code}",
    }


def test_youtube_client_comment_timeout_is_best_effort() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json=_video_payload())
        raise httpx.ReadTimeout("timeout")

    client = YouTubeDataApiClient(_settings(), transport=httpx.MockTransport(handler))

    result = _run(client.fetch_video("ZJMi3m8spJA"))

    assert result.uploader_comments == []
    assert result.comments_error == {
        "reason": "timeout",
        "message": "ReadTimeout",
    }
