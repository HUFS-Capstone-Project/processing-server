"""`parse_instagram_reel_meta` 회귀 테스트."""

from app.services.crawler.instagram_reel_parse import parse_instagram_reel_meta


def test_15k_likes_and_plain_comments():
    s = (
        '15K likes, 177 comments - boldpage.global - April 2, 2026: "가수 마크 스토리.\n\n#마크 #nct".'
    )
    r = parse_instagram_reel_meta(s)
    assert r is not None
    assert r["likes"] == 15000
    assert r["comments"] == 177
    assert r["username"] == "boldpage.global"
    assert r["posted_at"] == "April 2, 2026"
    assert "가수 마크" in r["caption"]


def test_comma_thousands_likes():
    s = (
        '1,428 likes, 16 comments - kyunghee_university - April 2, 2026: "[2026 경희.zip 📂]\n\n맛집 리스트.\n\n#경희대".'
    )
    r = parse_instagram_reel_meta(s)
    assert r is not None
    assert r["likes"] == 1428
    assert r["comments"] == 16
    assert r["username"] == "kyunghee_university"
    assert "[2026 경희.zip" in r["caption"]


def test_plain_numbers_and_emoji_caption():
    s = (
        '171 likes, 15 comments - guri_local - March 31, 2026: "🍶\n구리 숨은 로컬 이자카야!!\n\n#우니 #이자카야".'
    )
    r = parse_instagram_reel_meta(s)
    assert r is not None
    assert r["likes"] == 171
    assert r["comments"] == 15
    assert r["username"] == "guri_local"
    assert "🍶" in r["caption"]
    assert "구리 숨은 로컬" in r["caption"]


def test_multiline_caption_with_hashtags_and_emoji():
    s = (
        '99 likes, 5 comments - cafe_daily - February 14, 2026: "❤️ 브런치 추천\n\n🥐 크루아상\n☕ 라떼\n\n#브런치 #카페 #데이트".'
    )
    r = parse_instagram_reel_meta(s)
    assert r is not None
    assert r["likes"] == 99
    assert r["comments"] == 5
    assert "크루아상" in r["caption"]
    assert "#브런치" in r["caption"]


def test_unparsed_returns_none():
    assert parse_instagram_reel_meta("not instagram og format") is None
    assert parse_instagram_reel_meta("") is None
