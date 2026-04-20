"""Instagram-specific Playwright BrowserContext and OG extraction scripts."""

from __future__ import annotations

from dataclasses import dataclass

from playwright.async_api import Browser, BrowserContext, Page, Route

from app.core.config import Settings

INSTAGRAM_BROWSER_ARGS: tuple[str, ...] = ("--disable-blink-features=AutomationControlled",)
SUPPORTED_BLOCKED_RESOURCE_TYPES: frozenset[str] = frozenset({"image", "font", "media"})


@dataclass(slots=True)
class InstagramPageRouteStats:
    blocked_resource_count: int = 0

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
  if (c && !isGeneric(c)) return { source: "og:description", content: c };
  c = metaContent('meta[name="description"]');
  if (c && !isGeneric(c)) return { source: "description", content: c };
  c = metaContent('meta[property="og:title"]');
  if (c && !isGeneric(c)) return { source: "og:title", content: c };

  const scripts = document.querySelectorAll('script[type="application/ld+json"]');
  for (const s of scripts) {
    try {
      const j = JSON.parse(s.textContent || "{}");
      const cand = j.description
        || (Array.isArray(j["@graph"]) && j["@graph"][0] && j["@graph"][0].description);
      if (typeof cand === "string" && cand.trim() && !isGeneric(cand.trim())) {
        return { source: "ld+json", content: cand.trim() };
      }
    } catch (e) {}
  }
  return { source: "none", content: "" };
}
"""


def resolve_blocked_resource_types(settings: Settings) -> set[str]:
    return settings.instagram_block_resource_type_set & SUPPORTED_BLOCKED_RESOURCE_TYPES


def should_block_resource(resource_type: str, blocked_types: set[str]) -> bool:
    return resource_type in blocked_types


async def configure_instagram_page(page: Page, settings: Settings) -> InstagramPageRouteStats:
    blocked_types = resolve_blocked_resource_types(settings)
    stats = InstagramPageRouteStats()
    if not blocked_types:
        return stats

    async def _on_route(route: Route) -> None:
        resource_type = route.request.resource_type
        if should_block_resource(resource_type, blocked_types):
            stats.blocked_resource_count += 1
            await route.abort()
            return
        await route.continue_()

    await page.route("**/*", _on_route)
    return stats


async def new_instagram_browser_context(browser: Browser, settings: Settings) -> BrowserContext:
    return await browser.new_context(
        user_agent=settings.instagram_ua,
        locale=settings.instagram_locale,
    )
