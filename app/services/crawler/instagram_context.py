"""Instagram 전용 Playwright `BrowserContext` 생성 및 og 준비 판별 스크립트."""

from playwright.sync_api import Browser, BrowserContext

from app.core.config import Settings

INSTAGRAM_BROWSER_ARGS: tuple[str, ...] = ("--disable-blink-features=AutomationControlled",)

# og:description 등 의미 있는 값이 있는지(추출 가능한지) — wait_for_function 조건
OG_READY_PREDICATE_JS = r"""
() => {
  const metaContent = (sel) => {
    const el = document.querySelector(sel);
    const v = el && el.getAttribute("content");
    return v ? v.trim() : "";
  };
  const isGeneric = (s) => !s || /^Instagram$/i.test(s) || /^Instagram from Meta$/i.test(s);

  let c = metaContent('meta[property="og:description"]');
  if (c && !isGeneric(c)) return true;
  c = metaContent('meta[name="description"]');
  if (c && !isGeneric(c)) return true;
  c = metaContent('meta[property="og:title"]');
  if (c && !isGeneric(c)) return true;

  const scripts = document.querySelectorAll('script[type="application/ld+json"]');
  for (const s of scripts) {
    try {
      const j = JSON.parse(s.textContent || "{}");
      const cand = j.description
        || (Array.isArray(j["@graph"]) && j["@graph"][0] && j["@graph"][0].description);
      if (typeof cand === "string" && cand.trim() && !isGeneric(cand.trim())) return true;
    } catch (e) {}
  }
  return false;
}
"""

# 최종 문자열 추출(위 판별과 동일한 소스 우선순위)
OG_EXTRACTION_JS = r"""
() => {
  const metaContent = (sel) => {
    const el = document.querySelector(sel);
    const v = el && el.getAttribute("content");
    return v ? v.trim() : "";
  };
  const isGeneric = (s) => !s || /^Instagram$/i.test(s) || /^Instagram from Meta$/i.test(s);

  let c = metaContent('meta[property="og:description"]');
  if (c && !isGeneric(c)) return c;
  c = metaContent('meta[name="description"]');
  if (c && !isGeneric(c)) return c;
  c = metaContent('meta[property="og:title"]');
  if (c && !isGeneric(c)) return c;

  const scripts = document.querySelectorAll('script[type="application/ld+json"]');
  for (const s of scripts) {
    try {
      const j = JSON.parse(s.textContent || "{}");
      const cand = j.description
        || (Array.isArray(j["@graph"]) && j["@graph"][0] && j["@graph"][0].description);
      if (typeof cand === "string" && cand.trim() && !isGeneric(cand.trim())) return cand.trim();
    } catch (e) {}
  }
  return "";
}
"""


def new_instagram_browser_context(browser: Browser, settings: Settings) -> BrowserContext:
    return browser.new_context(
        user_agent=settings.instagram_ua,
        locale=settings.instagram_locale,
    )
