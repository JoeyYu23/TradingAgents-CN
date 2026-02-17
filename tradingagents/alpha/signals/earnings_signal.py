"""Earnings signal extraction from earnings history and upcoming dates."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from .signal_types import Signal

if TYPE_CHECKING:
    from ..data_collectors.yfinance_collector import StockData


def extract(data: StockData) -> Signal:
    """Extract earnings signal from surprise history and upcoming dates."""
    points: dict = {}
    scores: list[float] = []

    confidence = _assess_confidence(data)

    _score_surprise_history(data, scores, points)
    _score_earnings_proximity(data, scores, points)

    if not scores:
        return Signal(
            name="earnings",
            direction="neutral",
            strength=0.0,
            confidence=0.0,
            data_points=points,
            reasoning="No earnings data available",
        )

    avg_score = sum(scores) / len(scores)
    direction = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
    strength = min(abs(avg_score), 1.0)

    return Signal(
        name="earnings",
        direction=direction,
        strength=round(strength, 2),
        confidence=round(confidence, 2),
        data_points=points,
        reasoning=_build_reasoning(data, direction, points),
    )


def _assess_confidence(data: StockData) -> float:
    score = 0.0
    if data.last_4_surprises:
        score += 0.3 + min(len(data.last_4_surprises) * 0.1, 0.4)
    if data.next_earnings_date is not None:
        score += 0.3
    return min(score, 1.0)


def _score_surprise_history(data: StockData, scores: list[float], points: dict) -> None:
    """Score based on earnings surprise pattern."""
    if not data.last_4_surprises:
        return

    beats = 0
    misses = 0
    total_surprise = 0.0
    for s in data.last_4_surprises:
        pct = s.get("surprise_pct", 0)
        total_surprise += pct
        if pct > 0:
            beats += 1
        elif pct < 0:
            misses += 1

    points["earnings_beats"] = beats
    points["earnings_misses"] = misses
    points["avg_surprise_pct"] = round(total_surprise / len(data.last_4_surprises), 2)

    # Consistent beaters are bullish
    if beats == len(data.last_4_surprises) and beats >= 3:
        scores.append(0.6)
    elif beats >= 3:
        scores.append(0.4)
    elif misses >= 3:
        scores.append(-0.5)
    elif beats > misses:
        scores.append(0.2)
    elif misses > beats:
        scores.append(-0.2)


def _score_earnings_proximity(data: StockData, scores: list[float], points: dict) -> None:
    """Score event risk from upcoming earnings."""
    if data.days_to_earnings is None:
        return
    points["days_to_earnings"] = data.days_to_earnings

    # Earnings within 7 days = high event risk, adds uncertainty
    if data.days_to_earnings <= 3:
        points["event_risk"] = "very_high"
        # Don't add directional score, but increase strength of existing signals
        scores.append(0.0)  # Neutral but flagged
    elif data.days_to_earnings <= 7:
        points["event_risk"] = "high"
    elif data.days_to_earnings <= 14:
        points["event_risk"] = "moderate"
    elif data.days_to_earnings <= 30:
        points["event_risk"] = "low"


def _build_reasoning(data: StockData, direction: str, points: dict) -> str:
    parts = []
    beats = points.get("earnings_beats", 0)
    misses = points.get("earnings_misses", 0)
    total = beats + misses
    if total > 0:
        parts.append(f"{beats}/{total} beats")
    days = points.get("days_to_earnings")
    if days is not None:
        parts.append(f"earnings in {days}d")
    event_risk = points.get("event_risk")
    if event_risk:
        parts.append(f"event risk {event_risk}")
    detail = ", ".join(parts) if parts else "limited data"
    return f"Earnings {direction}: {detail}"
