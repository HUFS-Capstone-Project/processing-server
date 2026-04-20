from __future__ import annotations

from app.core.config import Settings
from app.services.crawler.instagram_context import (
    resolve_blocked_resource_types,
    should_block_resource,
)


def test_resolve_blocked_resource_types_filters_supported_values() -> None:
    settings = Settings(instagram_block_resource_types="image,font,media,script,foobar")
    resolved = resolve_blocked_resource_types(settings)
    assert resolved == {"image", "font", "media"}


def test_should_block_resource() -> None:
    blocked = {"image", "font"}
    assert should_block_resource("image", blocked) is True
    assert should_block_resource("media", blocked) is False
