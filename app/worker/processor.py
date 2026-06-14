from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from app.core.config import Settings
from app.domain.crawl import crawl_and_parse
from app.domain.job import (
    CrawlArtifact,
    PlaceSearchQuery,
    ExtractedPlace,
    ExtractionResult,
    JobRecord,
    PlaceCandidate,
    as_extraction_result_dict,
    as_place_dict,
    extracted_places_from_result,
)
from app.domain.job.service import INSTAGRAM_RATE_LIMITED_ERROR_CODE
from app.domain.url_contract import crawl_url_for, is_instagram_media_url, is_instagram_post_url
from app.infra.llm import HFExtractionError
from app.infra.kakao import KakaoNonRetryableError
from app.services.crawler.playwright_service import fetch_instagram_post_images
from app.services.crawler.extractors.registry import UnsupportedPlatformUrlError

logger = logging.getLogger("processing.worker.processor")

UNSUPPORTED_PLATFORM_URL_ERROR_CODE = "UNSUPPORTED_PLATFORM_URL"


def build_instagram_ocr_augmented_content(
    *,
    caption: str,
    ocr_texts: list[str],
) -> str:
    sections: list[tuple[str, str]] = []
    clean_caption = (caption or "").strip()
    if clean_caption:
        sections.append(("[caption]", clean_caption))

    clean_ocr_texts = [text.strip() for text in ocr_texts if text and text.strip()]
    if clean_ocr_texts:
        image_text = "\n\n".join(
            f"image {index}:\n{text}"
            for index, text in enumerate(clean_ocr_texts, 1)
        )
        sections.append(("[image_ocr]", image_text))

    return "\n\n".join(f"{header}\n{body}" for header, body in sections).strip()


@dataclass(slots=True)
class JobProcessOutcome:
    processed: bool
    succeeded: bool
    timed_out: bool
    elapsed_ms: int
    retryable: bool = False
    error_code: str | None = None
    error_message: str | None = None
    attempt_count: int = 0
    max_attempts: int = 0


class JobRepositoryPort(Protocol):
    async def claim_job(self, job_id: UUID) -> JobRecord | None: ...

    async def upsert_job_result(self, **kwargs): ...

    async def upsert_crawled_content(self, **kwargs): ...

    async def upsert_link_stats(self, **kwargs): ...

    async def mark_succeeded(self, job_id: UUID): ...

    async def mark_failed(
        self,
        job_id: UUID,
        error_message: str,
        error_code: str | None = None,
    ): ...


class ExtractionPort(Protocol):
    async def extract(
        self,
        *,
        text: str,
        original_url: str,
        media_type: str | None,
    ) -> ExtractionResult | None: ...


class OCRPort(Protocol):
    async def extract_texts_from_image_urls(self, image_urls: list[str]) -> list[str]: ...


class InstagramCooldownPort(Protocol):
    async def set_instagram_cooldown(self, seconds: int) -> None: ...

    async def instagram_cooldown_ttl(self) -> int: ...


class PlaceSearchResultPort(Protocol):
    places: list[PlaceCandidate]


class PlaceSearchPort(Protocol):
    async def search_places(
        self,
        candidate: PlaceSearchQuery,
        location_hints: list[str],
    ) -> PlaceSearchResultPort: ...


class JobProcessor:
    def __init__(
        self,
        *,
        repository: JobRepositoryPort,
        settings: Settings,
        extraction_client: ExtractionPort | None = None,
        ocr_client: OCRPort | None = None,
        place_search_client: PlaceSearchPort | None = None,
        cooldown_store: InstagramCooldownPort | None = None,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._extraction_client = extraction_client
        self._ocr_client = ocr_client
        self._place_search_client = place_search_client
        self._cooldown_store = cooldown_store

    async def process_job(self, job_id: UUID) -> JobProcessOutcome:
        started = time.monotonic()
        job = await self._repository.claim_job(job_id)
        if not job:
            logger.info("job skipped (not found or already claimed) job_id=%s", job_id)
            return JobProcessOutcome(
                processed=False,
                succeeded=False,
                timed_out=False,
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        logger.info("job claimed job_id=%s original_url=%s", job.job_id, job.original_url)

        try:
            cooldown_seconds = await self._instagram_cooldown_ttl(job.original_url)
            if cooldown_seconds > 0:
                error_message = (
                    "Instagram crawling is temporarily rate-limited. "
                    f"Retry after {cooldown_seconds} seconds."
                )
                await self._mark_failed(
                    job.job_id,
                    error_message,
                    error_code=INSTAGRAM_RATE_LIMITED_ERROR_CODE,
                )
                elapsed_ms = int((time.monotonic() - started) * 1000)
                return JobProcessOutcome(
                    processed=True,
                    succeeded=False,
                    timed_out=False,
                    elapsed_ms=elapsed_ms,
                    retryable=False,
                    error_code=INSTAGRAM_RATE_LIMITED_ERROR_CODE,
                    error_message=error_message,
                    attempt_count=job.attempt_count,
                    max_attempts=job.max_attempts,
                )

            crawl_artifact = await crawl_and_parse(job.original_url, self._settings)
            if self._is_instagram_rate_limited(crawl_artifact):
                cooldown_seconds = await self._set_instagram_cooldown()
                error_message = (
                    "Instagram crawl returned HTTP 429. "
                    f"Global cooldown started for {cooldown_seconds} seconds."
                )
                self._log_instagram_rate_limited(
                    job=job,
                    crawl_artifact=crawl_artifact,
                    cooldown_seconds=cooldown_seconds,
                )
                await self._persist_outputs(
                    job=job,
                    crawl_artifact=crawl_artifact,
                    extraction_result=None,
                    place_candidates=[],
                    resolved_places=[],
                )
                await self._mark_failed(
                    job.job_id,
                    error_message,
                    error_code=INSTAGRAM_RATE_LIMITED_ERROR_CODE,
                )
                elapsed_ms = int((time.monotonic() - started) * 1000)
                return JobProcessOutcome(
                    processed=True,
                    succeeded=False,
                    timed_out=False,
                    elapsed_ms=elapsed_ms,
                    retryable=False,
                    error_code=INSTAGRAM_RATE_LIMITED_ERROR_CODE,
                    error_message=error_message,
                    attempt_count=job.attempt_count,
                    max_attempts=job.max_attempts,
                )

            if self._is_empty_instagram_crawl(crawl_artifact):
                error_code = "EMPTY_INSTAGRAM_CRAWL"
                error_message = (
                    "Instagram crawl returned empty content and no OG source; "
                    "see crawled content raw_metadata for diagnostics."
                )
                logger.warning(
                    (
                        "job failed due to empty instagram crawl job_id=%s original_url=%s "
                        "og_source=%s response_status=%s html_len=%s body_text_len=%s "
                        "og_meta_count=%s login_form_present=%s challenge_marker_present=%s"
                    ),
                    job.job_id,
                    job.original_url,
                    self._instagram_metadata(crawl_artifact).get("og_source"),
                    (crawl_artifact.raw_metadata or {}).get("response_status"),
                    (crawl_artifact.raw_metadata or {}).get("html_len"),
                    (crawl_artifact.raw_metadata or {}).get("body_text_len"),
                    self._instagram_metadata(crawl_artifact).get("og_meta_count"),
                    self._instagram_metadata(crawl_artifact).get("login_form_present"),
                    self._instagram_metadata(crawl_artifact).get("challenge_marker_present"),
                )
                await self._persist_outputs(
                    job=job,
                    crawl_artifact=crawl_artifact,
                    extraction_result=None,
                    place_candidates=[],
                    resolved_places=[],
                )
                await self._mark_failed(job.job_id, error_message, error_code=error_code)
                elapsed_ms = int((time.monotonic() - started) * 1000)
                return JobProcessOutcome(
                    processed=True,
                    succeeded=False,
                    timed_out=False,
                    elapsed_ms=elapsed_ms,
                    retryable=True,
                    error_code=error_code,
                    error_message=error_message,
                    attempt_count=job.attempt_count,
                    max_attempts=job.max_attempts,
                )

            extraction_result = await self._extract_result(job.original_url, crawl_artifact)
            place_candidates, resolved_places = await self._enrich_place(
                extraction_result,
                crawl_artifact,
            )
            logger.info(
                "job crawl completed job_id=%s content_text_len=%s place_candidates=%s resolved_places=%s",
                job.job_id,
                len(crawl_artifact.content_text),
                len(place_candidates),
                len(resolved_places),
            )

            await self._persist_outputs(
                job=job,
                crawl_artifact=crawl_artifact,
                extraction_result=extraction_result,
                place_candidates=place_candidates,
                resolved_places=resolved_places,
            )
            await self._repository.mark_succeeded(job.job_id)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info("job succeeded job_id=%s elapsed_ms=%s", job.job_id, elapsed_ms)
            return JobProcessOutcome(
                processed=True,
                succeeded=True,
                timed_out=False,
                elapsed_ms=elapsed_ms,
            )
        except Exception as exc:
            logger.exception("job processing failed job_id=%s", job_id)
            error_code = self._error_code(exc)
            error_message = f"{exc.__class__.__name__}: {exc}"
            await self._mark_failed(job_id, error_message, error_code=error_code)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            timed_out = isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or (
                exc.__class__.__name__ == "PlaywrightTimeoutError"
            )
            return JobProcessOutcome(
                processed=True,
                succeeded=False,
                timed_out=timed_out,
                elapsed_ms=elapsed_ms,
                retryable=self._is_retryable_error(exc),
                error_code=error_code,
                error_message=error_message,
                attempt_count=job.attempt_count,
                max_attempts=job.max_attempts,
            )

    async def _persist_outputs(
        self,
        *,
        job: JobRecord,
        crawl_artifact: CrawlArtifact,
        extraction_result: ExtractionResult | None,
        place_candidates: list[dict[str, object]],
        resolved_places: list[dict[str, object]],
    ) -> None:
        await self._repository.upsert_crawled_content(
            job_id=job.job_id,
            crawl_url=crawl_artifact.url,
            source_type=crawl_artifact.source_type or "GENERIC_WEB",
            content_text=crawl_artifact.content_text,
            extraction_method=crawl_artifact.extraction_method,
            raw_metadata=crawl_artifact.raw_metadata,
        )
        if crawl_artifact.link_stats is not None:
            await self._repository.upsert_link_stats(
                job_id=job.job_id,
                **self._link_stats_kwargs(crawl_artifact.link_stats),
            )
        await self._repository.upsert_job_result(
            job_id=job.job_id,
            extraction_result=(
                as_extraction_result_dict(extraction_result) if extraction_result else None
            ),
            place_candidates=place_candidates,
            resolved_places=resolved_places,
        )

    async def _mark_failed(
        self,
        job_id: UUID,
        error_message: str,
        *,
        error_code: str | None = None,
    ) -> None:
        try:
            await self._repository.mark_failed(job_id, error_message, error_code=error_code)
        except TypeError:
            await self._repository.mark_failed(job_id, error_message)

    @staticmethod
    def _is_empty_instagram_crawl(crawl_artifact: CrawlArtifact) -> bool:
        instagram_metadata = JobProcessor._instagram_metadata(crawl_artifact)
        raw_metadata = crawl_artifact.raw_metadata or {}
        blocked = any(
            bool(instagram_metadata.get(key))
            for key in ("login_gate", "challenge", "generic_instagram_page")
        )
        return (
            crawl_artifact.source_type == "INSTAGRAM"
            and raw_metadata.get("response_status") == 200
            and not (crawl_artifact.content_text or "").strip()
            and (
                blocked
                or (
                    str(instagram_metadata.get("og_source") or "none").strip().lower() == "none"
                    and JobProcessor._safe_int(instagram_metadata.get("og_meta_count")) == 0
                    and JobProcessor._safe_int(raw_metadata.get("body_text_len")) == 0
                )
            )
        )

    @staticmethod
    def _is_instagram_rate_limited(crawl_artifact: CrawlArtifact) -> bool:
        return (
            crawl_artifact.source_type == "INSTAGRAM"
            and (crawl_artifact.raw_metadata or {}).get("response_status") == 429
        )

    async def _instagram_cooldown_ttl(self, original_url: str) -> int:
        if not self._cooldown_store or not is_instagram_media_url(original_url):
            return 0
        try:
            return max(0, int(await self._cooldown_store.instagram_cooldown_ttl()))
        except Exception:
            logger.warning("instagram cooldown ttl lookup failed", exc_info=True)
            return 0

    async def _set_instagram_cooldown(self) -> int:
        cooldown_seconds = max(1, int(self._settings.instagram_rate_limit_cooldown_seconds))
        if self._cooldown_store:
            try:
                await self._cooldown_store.set_instagram_cooldown(cooldown_seconds)
            except Exception:
                logger.warning("instagram cooldown set failed", exc_info=True)
        return cooldown_seconds

    def _log_instagram_rate_limited(
        self,
        *,
        job: JobRecord,
        crawl_artifact: CrawlArtifact,
        cooldown_seconds: int,
    ) -> None:
        raw_metadata = crawl_artifact.raw_metadata or {}
        instagram_metadata = self._instagram_metadata(crawl_artifact)
        logger.warning(
            (
                "job failed due to instagram rate limit job_id=%s original_url=%s "
                "crawl_url=%s response_status=%s response_url=%s final_url=%s "
                "html_len=%s body_text_len=%s og_meta_count=%s cooldown_seconds=%s"
            ),
            job.job_id,
            job.original_url,
            crawl_artifact.url or crawl_url_for(job.original_url),
            raw_metadata.get("response_status"),
            raw_metadata.get("response_url"),
            raw_metadata.get("final_url"),
            raw_metadata.get("html_len"),
            raw_metadata.get("body_text_len"),
            instagram_metadata.get("og_meta_count"),
            cooldown_seconds,
        )

    @staticmethod
    def _instagram_metadata(crawl_artifact: CrawlArtifact) -> dict:
        raw_metadata = crawl_artifact.raw_metadata or {}
        instagram_metadata = raw_metadata.get("instagram")
        if isinstance(instagram_metadata, dict):
            return instagram_metadata
        return raw_metadata

    @staticmethod
    def _safe_int(value: object) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    async def _extract_result(
        self,
        original_url: str,
        crawl_artifact: CrawlArtifact,
    ) -> ExtractionResult | None:
        if not self._extraction_client:
            return None

        extraction_result: ExtractionResult | None = None
        if crawl_artifact.content_text:
            extraction_result = await self._run_extraction(original_url, crawl_artifact)
            if self._has_extracted_places(extraction_result):
                return extraction_result

        fallback_result = await self._extract_with_instagram_image_fallback(
            original_url,
            crawl_artifact,
        )
        return fallback_result if fallback_result is not None else extraction_result

    async def _run_extraction(
        self,
        original_url: str,
        crawl_artifact: CrawlArtifact,
    ) -> ExtractionResult | None:
        try:
            return await self._extraction_client.extract(
                text=crawl_artifact.content_text,
                original_url=original_url,
                media_type=crawl_artifact.media_type,
            )
        except Exception:
            logger.exception("extraction failed original_url=%s", original_url)
            if self._settings.extraction_failure_retry_enabled:
                raise
            return None

    async def _extract_with_instagram_image_fallback(
        self,
        original_url: str,
        crawl_artifact: CrawlArtifact,
    ) -> ExtractionResult | None:
        if not self._ocr_client:
            return None
        if not self._should_run_instagram_image_fallback(original_url, crawl_artifact):
            return None

        try:
            image_result = await fetch_instagram_post_images(crawl_artifact.url, self._settings)
            ocr_texts = await self._ocr_client.extract_texts_from_image_urls(image_result.image_urls)
            augmented_text = build_instagram_ocr_augmented_content(
                caption=crawl_artifact.content_text,
                ocr_texts=ocr_texts,
            )
            self._record_instagram_ocr_fallback(
                crawl_artifact,
                image_count=len(image_result.image_urls),
                ocr_text_count=len(ocr_texts),
                image_fetch_timed_out=image_result.timed_out,
                image_fetch_error=image_result.error,
            )
            if not augmented_text:
                return None
            crawl_artifact.content_text = augmented_text
            return await self._run_extraction(original_url, crawl_artifact)
        except Exception:
            logger.exception("instagram image OCR fallback failed original_url=%s", original_url)
            if self._settings.extraction_failure_retry_enabled:
                raise
            return None

    @staticmethod
    def _has_extracted_places(result: ExtractionResult | None) -> bool:
        return bool(result and extracted_places_from_result(result))

    @staticmethod
    def _should_run_instagram_image_fallback(
        original_url: str,
        crawl_artifact: CrawlArtifact,
    ) -> bool:
        return (
            crawl_artifact.source_type == "INSTAGRAM"
            and crawl_artifact.media_type == "post"
            and is_instagram_post_url(original_url)
        )

    @staticmethod
    def _record_instagram_ocr_fallback(
        crawl_artifact: CrawlArtifact,
        *,
        image_count: int,
        ocr_text_count: int,
        image_fetch_timed_out: bool,
        image_fetch_error: str | None,
    ) -> None:
        raw_metadata = dict(crawl_artifact.raw_metadata or {})
        instagram_metadata = dict(raw_metadata.get("instagram") or {})
        instagram_metadata["ocr_fallback"] = {
            "attempted": True,
            "image_count": image_count,
            "ocr_text_count": ocr_text_count,
            "image_fetch_timed_out": image_fetch_timed_out,
            "image_fetch_error": image_fetch_error,
        }
        raw_metadata["instagram"] = instagram_metadata
        raw_metadata["extraction_source"] = "instagram_og_meta_with_image_ocr_fallback"
        crawl_artifact.raw_metadata = raw_metadata

    async def _enrich_place(
        self,
        extraction_result: ExtractionResult | None,
        crawl_artifact: CrawlArtifact,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        if not self._place_search_client or not extraction_result:
            return [], []

        extracted_places = extracted_places_from_result(extraction_result)
        if not extracted_places:
            return [], []

        all_places: list[PlaceCandidate] = []
        resolved_places: list[dict[str, object]] = []
        seen_candidate_keys: set[str] = set()
        seen_selected_keys: set[str] = set()

        max_places = max(1, self._settings.extraction_max_candidates)
        for extracted_place in extracted_places[:max_places]:
            candidate = self._build_extracted_candidate(extracted_place, crawl_artifact)
            if not candidate:
                continue
            location_hints = self._build_location_hints(extracted_place.address)

            try:
                places = await self._search_places_by_hints(candidate, location_hints)
            except KakaoNonRetryableError:
                logger.error("kakao enrichment non-retryable failure", exc_info=True)
                return [], []
            except Exception:
                logger.exception(
                    "kakao enrichment failed evidence_text=%s",
                    candidate.evidence_text,
                )
                continue

            places = sorted(places, key=lambda place: place.confidence, reverse=True)
            if places:
                selected_key = self._place_dedupe_key(places[0])
                if selected_key not in seen_selected_keys:
                    resolved_places.append(as_place_dict(places[0]))
                    seen_selected_keys.add(selected_key)

            for place in places:
                candidate_key = self._place_dedupe_key(place)
                if candidate_key in seen_candidate_keys:
                    continue
                all_places.append(place)
                seen_candidate_keys.add(candidate_key)

        place_candidates = [as_place_dict(place) for place in all_places]
        return place_candidates, resolved_places

    def _build_extracted_candidate(
        self,
        extracted_place: ExtractedPlace,
        crawl_artifact: CrawlArtifact,
    ) -> PlaceSearchQuery | None:
        store_name = (extracted_place.store_name or "").strip()
        if not store_name:
            return None
        return PlaceSearchQuery(
            query=store_name,
            evidence_text=(
                extracted_place.store_name_evidence
                or extracted_place.address_evidence
                or crawl_artifact.content_text
                or ""
            ),
            original_text=store_name,
        )

    @staticmethod
    def _place_dedupe_key(place: PlaceCandidate) -> str:
        if place.kakao_place_id:
            return f"id:{place.kakao_place_id}"
        return f"name:{place.place_name}|{place.address_name}|{place.road_address_name}"

    async def _search_places(
        self,
        candidate: PlaceSearchQuery,
        location_hints: list[str],
    ) -> list[PlaceCandidate]:
        if not self._place_search_client:
            return []
        result = await self._place_search_client.search_places(candidate, location_hints)
        return result.places

    async def _search_places_by_hints(
        self,
        candidate: PlaceSearchQuery,
        location_hints: list[str],
    ) -> list[PlaceCandidate]:
        for hint in location_hints:
            places = await self._search_places(candidate, [hint])
            qualified = self._qualified_places(places)
            if qualified:
                return qualified

        places = await self._search_places(candidate, [])
        qualified = self._qualified_places(places)
        if qualified:
            return qualified

        for hint in location_hints:
            address_candidate = PlaceSearchQuery(
                query=hint,
                evidence_text=candidate.evidence_text,
                original_text=candidate.original_text,
            )
            places = await self._search_places(address_candidate, [hint])
            qualified = self._qualified_places(places)
            if qualified:
                return qualified

        return []

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        if isinstance(exc, HFExtractionError):
            return True
        if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            return True
        name = exc.__class__.__name__.lower()
        message = str(exc).lower()
        retryable_fragments = (
            "timeout",
            "temporar",
            "network",
            "connection",
            "browser",
            "closed",
            "retryable",
        )
        return any(fragment in name or fragment in message for fragment in retryable_fragments)

    @staticmethod
    def _error_code(exc: Exception) -> str:
        if isinstance(exc, UnsupportedPlatformUrlError):
            return UNSUPPORTED_PLATFORM_URL_ERROR_CODE
        if isinstance(exc, HFExtractionError):
            return "RETRYABLE_EXTRACTION_ERROR"
        if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            return "RETRYABLE_TIMEOUT"
        name = exc.__class__.__name__
        if "Playwright" in name or "Browser" in name:
            return "RETRYABLE_CRAWLER_ERROR"
        return "JOB_PROCESSING_FAILED"

    def _qualified_places(self, places: list[PlaceCandidate]) -> list[PlaceCandidate]:
        return [
            place
            for place in places
            if place.confidence >= self._settings.kakao_min_place_confidence
        ]

    @staticmethod
    def _build_location_hints(address: str | None) -> list[str]:
        raw = (address or "").strip()
        if not raw:
            return []

        tokens = [token.strip(",") for token in re.split(r"\s+", raw) if token.strip(",")]
        hints = [raw]

        district_suffixes = ("\uad6c", "\uad70")
        locality_suffixes = ("\ub3d9", "\uc74d", "\uba74", "\ub9ac", "\uac00")

        district_idx = next(
            (idx for idx, token in enumerate(tokens) if token.endswith(district_suffixes)),
            None,
        )
        if district_idx is not None:
            hints.append(" ".join(tokens[: district_idx + 1]))

            locality_idx = next(
                (
                    idx
                    for idx in range(district_idx + 1, len(tokens))
                    if tokens[idx].endswith(locality_suffixes)
                ),
                None,
            )
            if locality_idx is not None:
                hints.append(" ".join(tokens[: locality_idx + 1]))

            road_hint = JobProcessor._build_road_hint(tokens, district_idx)
            if road_hint:
                hints.append(road_hint)

        deduped: list[str] = []
        for hint in hints:
            if hint and hint not in deduped:
                deduped.append(hint)
        return deduped

    @staticmethod
    def _build_road_hint(tokens: list[str], district_idx: int) -> str | None:
        prefix = tokens[: district_idx + 1]
        rest = tokens[district_idx + 1 :]
        if not prefix or not rest:
            return None

        for idx, token in enumerate(rest):
            if token.endswith("\uae38"):
                return " ".join(prefix + [token])
            if token.endswith("\ub85c"):
                if idx + 1 < len(rest) and re.fullmatch(r"\d+\uae38", rest[idx + 1]):
                    return " ".join(prefix + [f"{token}{rest[idx + 1]}"])
                return " ".join(prefix + [token])
        return None

    @staticmethod
    def _link_stats_kwargs(link_stats) -> dict[str, object]:
        stats_source = getattr(link_stats, "stats_source", None)
        confidence = getattr(link_stats, "confidence", None)
        source_type = getattr(link_stats, "source_type", None)
        return {
            "crawl_url": getattr(link_stats, "source_url"),
            "source_type": getattr(source_type, "value", source_type),
            "like_count": getattr(link_stats, "like_count", None),
            "comment_count": getattr(link_stats, "comment_count", None),
            "posted_at": getattr(link_stats, "posted_at", None),
            "collected_at": getattr(link_stats, "collected_at", None),
            "stats_source": getattr(stats_source, "value", stats_source),
            "confidence": getattr(confidence, "value", confidence),
            "unavailable_reason": getattr(link_stats, "unavailable_reason", None),
            "raw_stats": getattr(link_stats, "raw_stats", None),
        }
