"""Technical signal extraction from price, MA, and RSI data."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .signal_types import Signal

if TYPE_CHECKING:
    from ..data_collectors.yfinance_collector import StockData


def extract(data: StockData) -> Signal:
    """Extract technical signal from price action, moving averages, and RSI."""
    points: dict = {}
    scores: list[float] = []  # positive = bullish, negative = bearish

    confidence = _assess_confidence(data)

    _score_rsi(data, scores, points)
    _score_moving_averages(data, scores, points)
    _score_52w_position(data, scores, points)

    if not scores:
        return Signal(
            name="technical",
            direction="neutral",
            strength=0.0,
            confidence=0.0,
            data_points=points,
            reasoning="Insufficient technical data",
        )

    avg_score = sum(scores) / len(scores)
    direction = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
    strength = min(abs(avg_score), 1.0)

    return Signal(
        name="technical",
        direction=direction,
        strength=round(strength, 2),
        confidence=round(confidence, 2),
        data_points=points,
        reasoning=_build_reasoning(data, direction),
    )


def _assess_confidence(data: StockData) -> float:
    """Assess data quality for technical indicators."""
    score = 0.0
    if data.history_30d is not None and len(data.history_30d) >= 20:
        score += 0.4
    if data.rsi_14 > 0:
        score += 0.2
    if data.ma_50 > 0:
        score += 0.2
    if data.ma_200 > 0:
        score += 0.2
    return score


def _score_rsi(data: StockData, scores: list[float], points: dict) -> None:
    """Score RSI: overbought/oversold signal."""
    if data.rsi_14 <= 0:
        return
    points["rsi_14"] = data.rsi_14
    if data.rsi_14 > 80:
        scores.append(-0.8)
    elif data.rsi_14 > 70:
        scores.append(-0.5)
    elif data.rsi_14 < 20:
        scores.append(0.8)
    elif data.rsi_14 < 30:
        scores.append(0.5)
    elif 45 <= data.rsi_14 <= 55:
        scores.append(0.0)
    elif data.rsi_14 > 55:
        scores.append(0.2)
    else:
        scores.append(-0.2)


def _score_moving_averages(data: StockData, scores: list[float], points: dict) -> None:
    """Score price relative to 50MA and 200MA, and golden/death cross."""
    price = data.current_price
    if price <= 0:
        return

    if data.ma_50 > 0:
        pct_vs_50 = (price - data.ma_50) / data.ma_50
        points["pct_vs_50ma"] = round(pct_vs_50 * 100, 1)
        if pct_vs_50 > 0.10:
            scores.append(0.4)
        elif pct_vs_50 > 0:
            scores.append(0.2)
        elif pct_vs_50 > -0.10:
            scores.append(-0.2)
        else:
            scores.append(-0.4)

    if data.ma_200 > 0:
        pct_vs_200 = (price - data.ma_200) / data.ma_200
        points["pct_vs_200ma"] = round(pct_vs_200 * 100, 1)
        if pct_vs_200 > 0.15:
            scores.append(0.5)
        elif pct_vs_200 > 0:
            scores.append(0.3)
        elif pct_vs_200 > -0.15:
            scores.append(-0.3)
        else:
            scores.append(-0.5)

    # Golden / death cross
    if data.ma_50 > 0 and data.ma_200 > 0:
        if data.ma_50 > data.ma_200:
            points["ma_cross"] = "golden"
            scores.append(0.3)
        else:
            points["ma_cross"] = "death"
            scores.append(-0.3)


def _score_52w_position(data: StockData, scores: list[float], points: dict) -> None:
    """Score position within 52-week range."""
    if data.week52_high <= 0 or data.week52_low <= 0:
        return
    price = data.current_price
    range_52w = data.week52_high - data.week52_low
    if range_52w <= 0:
        return
    position = (price - data.week52_low) / range_52w
    points["52w_position_pct"] = round(position * 100, 1)
    points["pct_from_52w_high"] = round(data.pct_from_52w_high, 1)

    if position > 0.95:
        scores.append(-0.3)  # Near highs, potential resistance
    elif position < 0.2:
        scores.append(0.3)  # Near lows, potential support


def _build_reasoning(data: StockData, direction: str) -> str:
    """Build a one-line reasoning string."""
    parts = []
    if data.rsi_14 > 0:
        parts.append(f"RSI {data.rsi_14:.0f}")
    if data.ma_50 > 0:
        parts.append(f"{'above' if data.current_price > data.ma_50 else 'below'} 50MA")
    if data.ma_200 > 0:
        parts.append(f"{'above' if data.current_price > data.ma_200 else 'below'} 200MA")
    detail = ", ".join(parts) if parts else "limited data"
    return f"Technical {direction}: {detail}"
