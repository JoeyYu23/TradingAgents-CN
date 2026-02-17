"""BubbleSeek scraper for stock news, KOL opinions, and options anomalies."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from .store import NewsItem

logger = logging.getLogger(__name__)

# Common English words that look like ticker symbols but aren't
_TICKER_BLOCKLIST = frozenset({
    "A", "I", "AM", "PM", "US", "CEO", "CFO", "CTO", "IPO",
    "THE", "AND", "FOR", "NOT", "ALL", "NEW", "OLD", "BIG",
    "TOP", "LOW", "HIGH", "ETF", "FDA", "SEC", "API", "AI",
    "EV", "PE", "IT", "OR", "IS", "AT", "BY", "TO", "IN",
    "ON", "UP", "DO", "GO", "SO", "NO", "IF", "AN", "AS",
    "OK", "VS", "PT", "DD", "EPS", "GDP", "CPI", "PMI",
    "DTE", "HOT", "BID", "ASK",
})

_TICKER_RE = re.compile(r"\$([A-Z]{1,5})\b")
_TICKER_FALLBACK_RE = re.compile(r"\b([A-Z]{2,5})\b")

# BubbleSeek event type -> our category
_TYPE_MAP: dict[str, str] = {
    "macro_news": "macro",
    "stock_news": "stock_news",
    "kol_tweet": "kol",
    "options_alert": "options_anomaly",
}


class BubbleSeekScraper:
    """Scraper for BubbleSeek using Playwright with API response interception."""

    URL = "https://bubbleseek.ai"

    async def fetch_latest(self, max_items: int = 30) -> list[NewsItem]:
        """Fetch latest items by intercepting BubbleSeek API responses."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Playwright not installed, BubbleSeek scraping unavailable")
            return []

        items: list[NewsItem] = []
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                page = await browser.new_page()
                try:
                    api_responses = await self._intercept_api_responses(page)
                    for raw in api_responses[:max_items]:
                        parsed = self._parse_event(raw)
                        if parsed is not None:
                            items.append(parsed)
                finally:
                    await browser.close()
        except Exception as exc:
            logger.error("BubbleSeek scraping failed: %s", exc)

        logger.info("BubbleSeek returned %d items", len(items))
        return items

    async def _intercept_api_responses(self, page) -> list[dict]:
        """Navigate to BubbleSeek and collect API response data."""
        collected: list[dict] = []

        async def _on_response(response) -> None:
            if "events/public" not in response.url:
                return
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type:
                return
            try:
                body = await response.json()
                if not isinstance(body, dict) or not body.get("success"):
                    return
                events = body.get("data", {}).get("events", [])
                if isinstance(events, list):
                    collected.extend(events)
            except (ValueError, TypeError, KeyError):
                pass

        page.on("response", _on_response)
        await page.goto(self.URL, timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=20000)
        return collected

    def _parse_event(self, event: dict) -> NewsItem | None:
        """Parse a BubbleSeek event into a NewsItem.

        Event structure:
            {id, type, data: {title, translatedContent, originalContent,
             importance, tags, symbols, content, embedContent}, timestamp, metadata}
        """
        event_type = event.get("type", "")
        category = _TYPE_MAP.get(event_type, "stock_news")
        data = event.get("data", {})
        if not isinstance(data, dict):
            return None

        title = str(data.get("title", "")).strip()
        content = str(
            data.get("translatedContent", "")
            or data.get("originalContent", "")
            or data.get("content", "")
        ).strip()

        if not title and not content:
            return None

        title = title or content[:120]
        content = content or title

        # Extract ticker from symbols field or content
        ticker = self._extract_ticker_from_event(data, title, content)

        # Importance: BubbleSeek uses 1-10 scale
        importance = self._map_importance(data.get("importance"), category)

        # Timestamp
        ts = event.get("timestamp", "")
        published = self._parse_timestamp(ts)

        return NewsItem(
            source="bubbleseek",
            category=category,
            title=title[:200],
            content=content,
            ticker=ticker,
            published_at=published,
            scraped_at=datetime.now(tz=timezone.utc),
            importance=importance,
            raw_data=event,
        )

    def _extract_ticker_from_event(
        self, data: dict, title: str, content: str,
    ) -> str:
        """Extract ticker from structured symbols field or text content."""
        # Prefer structured symbols field
        symbols = data.get("symbols")
        if symbols and isinstance(symbols, list):
            for sym in symbols:
                s = str(sym).strip().upper()
                if s and s not in _TICKER_BLOCKLIST:
                    return s

        # Look for $TICKER pattern first (most reliable)
        text = f"{title} {content}"
        dollar_matches = _TICKER_RE.findall(text)
        for match in dollar_matches:
            if match not in _TICKER_BLOCKLIST:
                return match

        # Fallback to bare uppercase words (only for stock_news/options)
        fallback_matches = _TICKER_FALLBACK_RE.findall(text)
        for match in fallback_matches:
            if match not in _TICKER_BLOCKLIST and len(match) >= 2:
                return match

        return ""

    @staticmethod
    def _map_importance(raw_importance: int | None, category: str) -> str:
        """Map BubbleSeek's 1-10 importance to high/medium/low."""
        if raw_importance is not None:
            if raw_importance >= 7:
                return "high"
            if raw_importance >= 4:
                return "medium"
            return "low"
        # Fallback based on category
        if category == "options_anomaly":
            return "high"
        if category == "kol":
            return "medium"
        return "low"

    @staticmethod
    def _parse_timestamp(time_str: str) -> datetime:
        """Best-effort timestamp parsing with UTC fallback."""
        if not time_str:
            return datetime.now(tz=timezone.utc)
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                return datetime.strptime(str(time_str), fmt).replace(
                    tzinfo=timezone.utc,
                )
            except (ValueError, TypeError):
                continue
        return datetime.now(tz=timezone.utc)
