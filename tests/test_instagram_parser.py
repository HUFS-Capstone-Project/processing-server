"""Regression tests for parse_instagram_reel_meta."""

from app.services.crawler.instagram_reel_parse import parse_instagram_reel_meta


def test_15k_likes_and_plain_comments() -> None:
    text = '15K likes, 177 comments - boldpage.global - April 2, 2026: "great brunch spot in seoul".'
    parsed = parse_instagram_reel_meta(text)

    assert parsed is not None
    assert parsed["likes"] == 15000
    assert parsed["comments"] == 177
    assert parsed["username"] == "boldpage.global"
    assert parsed["posted_at"] == "April 2, 2026"
    assert "great brunch" in parsed["caption"]


def test_comma_thousands_likes() -> None:
    text = '1,428 likes, 16 comments - kyunghee_university - April 2, 2026: "campus cafe update".'
    parsed = parse_instagram_reel_meta(text)

    assert parsed is not None
    assert parsed["likes"] == 1428
    assert parsed["comments"] == 16
    assert parsed["username"] == "kyunghee_university"
    assert "campus cafe" in parsed["caption"]


def test_plain_numbers_and_emoji_caption() -> None:
    text = '171 likes, 15 comments - guri_local - March 31, 2026: "late-night izakaya".'
    parsed = parse_instagram_reel_meta(text)

    assert parsed is not None
    assert parsed["likes"] == 171
    assert parsed["comments"] == 15
    assert parsed["username"] == "guri_local"
    assert "izakaya" in parsed["caption"]


def test_multiline_caption_with_hashtags_and_emoji() -> None:
    text = '99 likes, 5 comments - cafe_daily - February 14, 2026: "brunch menu\n\n#brunch #cafe".'
    parsed = parse_instagram_reel_meta(text)

    assert parsed is not None
    assert parsed["likes"] == 99
    assert parsed["comments"] == 5
    assert "#brunch" in parsed["caption"]


def test_unparsed_returns_none() -> None:
    assert parse_instagram_reel_meta("not instagram og format") is None
    assert parse_instagram_reel_meta("") is None
