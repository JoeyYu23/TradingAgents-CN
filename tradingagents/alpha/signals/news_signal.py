"""News sentiment signal extraction from scraped news data."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .signal_types import Signal

if TYPE_CHECKING:
    from ..data_collectors.news_collector import NewsData


def extract(news_data: NewsData) -> Signal:
    """Extract news sentiment signal from scraped news data."""
    points: dict = {}
    scores: list[float] = []

    confidence = _assess_confidence(news_data)

    _score_ticker_sentiment(news_data, scores, points)
    _score_macro_sentiment(news_data, scores, points)
    _score_volume_anomaly(news_data, scores, points)

    if not scores:
        return Signal(
            name="news_sentiment",
            direction="neutral",
            strength=0.0,
            confidence=0.0,
            data_points=points,
            reasoning="No news data available (daemon may not be running)",
        )

    avg_score = sum(scores) / len(scores)
    direction = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
    strength = min(abs(avg_score), 1.0)

    return Signal(
        name="news_sentiment",
        direction=direction,
        strength=round(strength, 2),
        confidence=round(confidence, 2),
        data_points=points,
        reasoning=_build_reasoning(points, direction),
    )


def _assess_confidence(news_data: NewsData) -> float:
    """Assess data quality based on volume and coverage."""
    if news_data.news_volume == 0:
        return 0.0

    score = 0.4
    if news_data.news_volume >= 10:
        score += 0.2
    if news_data.high_importance_count > 0:
        score += 0.2
    if news_data.ticker_news and news_data.macro_news:
        score += 0.2
    return min(score, 1.0)


def _score_ticker_sentiment(
    news_data: NewsData, scores: list[float], points: dict,
) -> None:
    """Score based on ticker-specific news sentiment."""
    points["ticker_sentiment"] = news_data.ticker_sentiment_score
    points["ticker_news_count"] = len(news_data.ticker_news)

    ts = news_data.ticker_sentiment_score
    if ts > 0.3:
        scores.append(0.6)
    elif ts > 0.1:
        scores.append(0.3)
    elif ts < -0.3:
        scores.append(-0.6)
    elif ts < -0.1:
        scores.append(-0.3)


def _score_macro_sentiment(
    news_data: NewsData, scores: list[float], points: dict,
) -> None:
    """Score based on macro news sentiment, weighting high-importance events."""
    points["macro_sentiment"] = news_data.macro_sentiment_score
    points["macro_news_count"] = len(news_data.macro_news)

    ms = news_data.macro_sentiment_score
    weight = 2.0 if news_data.high_importance_count > 0 else 1.0

    if ms > 0.3:
        scores.append(min(0.5 * weight, 1.0))
    elif ms > 0.1:
        scores.append(min(0.25 * weight, 1.0))
    elif ms < -0.3:
        scores.append(max(-0.5 * weight, -1.0))
    elif ms < -0.1:
        scores.append(max(-0.25 * weight, -1.0))


def _score_volume_anomaly(
    news_data: NewsData, scores: list[float], points: dict,
) -> None:
    """Amplify signal when news volume is unusually high."""
    points["news_volume"] = news_data.news_volume

    if news_data.news_volume <= 50:
        return

    # Determine overall sentiment direction for amplification
    combined = news_data.ticker_sentiment_score + news_data.macro_sentiment_score
    if combined > 0:
        scores.append(0.3)
    elif combined < 0:
        scores.append(-0.3)


def _build_reasoning(points: dict, direction: str) -> str:
    parts = []
    ts = points.get("ticker_sentiment", 0)
    if ts != 0:
        parts.append(f"ticker_sentiment={ts:.2f}")
    volume = points.get("news_volume", 0)
    if volume > 0:
        parts.append(f"{volume} articles")
    ticker_count = points.get("ticker_news_count", 0)
    if ticker_count > 0:
        parts.append(f"{ticker_count} ticker-specific")
    macro_count = points.get("macro_news_count", 0)
    if macro_count > 0:
        parts.append(f"{macro_count} macro")
    detail = ", ".join(parts) if parts else "no recent news"
    return f"News {direction}: {detail}"
