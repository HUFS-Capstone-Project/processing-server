"""Instagram-specific Playwright BrowserContext and OG extraction scripts."""

from playwright.sync_api import Browser, BrowserContext

from app.core.config import Settings

INSTAGRAM_BROWSER_ARGS: tuple[str, ...] = ("--disable-blink-features=AutomationControlled",)

# wait_for_function predicate: true when any meaningful OG/description source is available.
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

# extraction script with the same source priority as readiness predicate.
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
