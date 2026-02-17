"""Jin10 flash news scraper for macro-economic events."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

import requests

from .store import NewsItem

logger = logging.getLogger(__name__)

# Keywords that indicate high-importance macro news
_HIGH_KEYWORDS = (
    "\u592e\u884c", "\u5229\u7387", "\u975e\u519c", "CPI", "GDP", "PMI",
    "\u7f8e\u8054\u50a8", "\u52a0\u606f", "\u964d\u606f",
)
# Keywords that indicate medium-importance macro news
_MEDIUM_KEYWORDS = ("\u9884\u671f", "\u516c\u5e03", "\u524d\u503c")

_HTML_TAG_RE = re.compile(r"<[^>]+>")
# Jin10 timestamps are in China Standard Time (UTC+8)
_CST = timezone(timedelta(hours=8))


class Jin10Scraper:
    """Scraper for Jin10 flash news API with Playwright fallback."""

    API_URL = "https://flash-api.jin10.com/get_flash_list"
    # Public app ID used by Jin10's web client (not a secret credential)
    HEADERS = {
        "x-app-id": "bVBF4FyRTn5NJF5n",
        "x-version": "1.0.0",
    }

    def fetch_latest(self, max_items: int = 20) -> list[NewsItem]:
        """Fetch latest flash news items from Jin10 API.

        Falls back to Playwright-based scraping if the API returns
        a non-200 status or times out.
        """
        try:
            resp = requests.get(
                self.API_URL,
                headers=self.HEADERS,
                params={"channel": "-8200", "max_time": "", "vip": "1"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            items: list[NewsItem] = []
            for raw in data[:max_items]:
                parsed = self._parse_flash_item(raw)
                if parsed is not None:
                    items.append(parsed)
            logger.info("Jin10 API returned %d items", len(items))
            return items
        except (requests.RequestException, ValueError, KeyError) as exc:
            logger.warning("Jin10 API failed (%s), Playwright fallback available via async", exc)
            return []

    async def fetch_latest_with_fallback(self, max_items: int = 20) -> list[NewsItem]:
        """Async version that tries HTTP API first, then Playwright fallback."""
        items = self.fetch_latest(max_items)
        if items:
            return items
        try:
            return await self.fetch_latest_playwright(max_items)
        except Exception as exc:
            logger.error("Jin10 Playwright fallback also failed: %s", exc)
            return []

    def _parse_flash_item(self, raw: dict) -> NewsItem | None:
        """Parse a single Jin10 flash JSON object into a NewsItem."""
        content_raw = raw.get("data", {}).get("content", "")
        content = _HTML_TAG_RE.sub("", content_raw).strip()
        if not content:
            return None

        time_str = raw.get("time", "")
        try:
            published = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=_CST,
            )
        except (ValueError, TypeError):
            published = datetime.now(tz=timezone.utc)

        return NewsItem(
            source="jin10",
            category="macro",
            title=content[:120],
            content=content,
            ticker="",
            published_at=published,
            scraped_at=datetime.now(tz=timezone.utc),
            importance=self._assess_importance(raw),
            raw_data=raw,
        )

    def _assess_importance(self, item: dict) -> str:
        """Classify importance based on flags and keyword matching."""
        if item.get("important") == 1:
            return "high"

        content = item.get("data", {}).get("content", "")
        if any(kw in content for kw in _HIGH_KEYWORDS):
            return "high"
        if any(kw in content for kw in _MEDIUM_KEYWORDS):
            return "medium"
        return "low"

    async def fetch_latest_playwright(
        self, max_items: int = 20,
    ) -> list[NewsItem]:
        """Fallback scraper using Playwright to render jin10.com."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Playwright not installed, cannot use fallback")
            return []

        items: list[NewsItem] = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto("https://www.jin10.com/", timeout=20000)
                await page.wait_for_selector(
                    ".jin_flash_item", timeout=15000,
                )
                elements = await page.query_selector_all(".jin_flash_item")
                for el in elements[:max_items]:
                    text = (await el.inner_text()).strip()
                    if not text:
                        continue
                    items.append(
                        NewsItem(
                            source="jin10",
                            category="macro",
                            title=text[:120],
                            content=text,
                            ticker="",
                            published_at=datetime.now(tz=timezone.utc),
                            scraped_at=datetime.now(tz=timezone.utc),
                            importance="low",
                            raw_data={"text": text},
                        )
                    )
            finally:
                await browser.close()

        logger.info("Jin10 Playwright returned %d items", len(items))
        return items
