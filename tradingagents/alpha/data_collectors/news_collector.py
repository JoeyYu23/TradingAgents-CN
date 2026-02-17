"""News data collector for the Alpha Contradiction Engine.

Reads scraped news from the local SQLite store (populated by the news daemon)
and computes basic sentiment scores using keyword matching.
Gracefully degrades when the store is empty or unavailable.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..scrapers.store import NewsItem

logger = logging.getLogger(__name__)

_BULLISH_KEYWORDS: set[str] = {
    "beat", "beats", "upgrade", "upgraded", "buy", "bullish",
    "outperform", "record", "growth", "surge", "rally",
    "breakout", "strong", "exceeded",
}

_BEARISH_KEYWORDS: set[str] = {
    "miss", "missed", "downgrade", "downgraded", "sell", "bearish",
    "underperform", "risk", "decline", "drop", "crash",
    "weak", "warning", "cut",
}

_IMPORTANCE_WEIGHTS: dict[str, float] = {
    "high": 2.0,
    "medium": 1.0,
    "low": 0.5,
}


def _make_keyword_pattern(keywords: set[str]) -> re.Pattern[str]:
    escaped = [re.escape(kw) for kw in sorted(keywords)]
    return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)


_BULLISH_RE = _make_keyword_pattern(_BULLISH_KEYWORDS)
_BEARISH_RE = _make_keyword_pattern(_BEARISH_KEYWORDS)


@dataclass
class NewsData:
    """Container for news data retrieved from the local store."""

    ticker_news: list[NewsItem] = field(default_factory=list)
    macro_news: list[NewsItem] = field(default_factory=list)
    ticker_sentiment_score: float = 0.0
    macro_sentiment_score: float = 0.0
    news_volume: int = 0
    high_importance_count: int = 0


def collect(ticker: str, hours_back: int = 24) -> NewsData:
    """Read from local SQLite store and return structured NewsData.

    Returns an empty NewsData with zeros if the store is unavailable or empty.
    """
    try:
        from ..scrapers.store import NewsStore

        with NewsStore() as store:
            ticker_news = store.query(ticker=ticker, hours_back=hours_back)
            macro_news = store.query_macro(hours_back=hours_back)
    except Exception as exc:
        logger.debug("NewsStore unavailable, returning empty data: %s", exc)
        return NewsData()

    all_items = ticker_news + macro_news
    high_count = sum(1 for item in all_items if item.importance == "high")

    return NewsData(
        ticker_news=ticker_news,
        macro_news=macro_news,
        ticker_sentiment_score=_score_items(ticker_news),
        macro_sentiment_score=_score_items(macro_news),
        news_volume=len(all_items),
        high_importance_count=high_count,
    )


def _score_items(items: list[NewsItem]) -> float:
    """Compute a sentiment score in [-1.0, 1.0] from keyword matching.

    Each item's title+content is scanned for bullish/bearish keywords
    using word-boundary matching. Scores are weighted by importance.
    """
    if not items:
        return 0.0

    total_score = 0.0
    total_weight = 0.0

    for item in items:
        text = f"{item.title} {item.content}"
        weight = _IMPORTANCE_WEIGHTS.get(item.importance, 1.0)

        bull_count = len(_BULLISH_RE.findall(text))
        bear_count = len(_BEARISH_RE.findall(text))

        if bull_count + bear_count == 0:
            continue

        item_score = (bull_count - bear_count) / (bull_count + bear_count)
        total_score += item_score * weight
        total_weight += weight

    if total_weight == 0.0:
        return 0.0

    raw = total_score / total_weight
    return max(-1.0, min(1.0, raw))
