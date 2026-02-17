"""Momentum signal extraction from price trend and volume confirmation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .signal_types import Signal

if TYPE_CHECKING:
    from ..data_collectors.yfinance_collector import StockData


def extract(data: StockData) -> Signal:
    """Extract momentum signal from price trend and volume analysis."""
    points: dict = {}
    scores: list[float] = []

    confidence = _assess_confidence(data)

    _score_price_trend(data, scores, points)
    _score_volume_confirmation(data, scores, points)
    _score_beta_momentum(data, scores, points)

    if not scores:
        return Signal(
            name="momentum",
            direction="neutral",
            strength=0.0,
            confidence=0.0,
            data_points=points,
            reasoning="Insufficient momentum data",
        )

    avg_score = sum(scores) / len(scores)
    direction = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
    strength = min(abs(avg_score), 1.0)

    return Signal(
        name="momentum",
        direction=direction,
        strength=round(strength, 2),
        confidence=round(confidence, 2),
        data_points=points,
        reasoning=_build_reasoning(direction, points),
    )


def _assess_confidence(data: StockData) -> float:
    if data.history_30d is None or len(data.history_30d) < 10:
        return 0.0
    score = 0.4
    if len(data.history_30d) >= 20:
        score += 0.3
    if "Volume" in data.history_30d.columns:
        score += 0.3
    return min(score, 1.0)


def _score_price_trend(data: StockData, scores: list[float], points: dict) -> None:
    """Score short-term and medium-term price trend."""
    if data.history_30d is None or len(data.history_30d) < 5:
        return

    close = data.history_30d["Close"]

    # 5-day return
    if len(close) >= 5:
        ret_5d = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5]
        points["return_5d_pct"] = round(ret_5d * 100, 2)
        if ret_5d > 0.05:
            scores.append(0.5)
        elif ret_5d > 0.02:
            scores.append(0.3)
        elif ret_5d > -0.02:
            scores.append(0.0)
        elif ret_5d > -0.05:
            scores.append(-0.3)
        else:
            scores.append(-0.5)

    # 20-day return (if available)
    if len(close) >= 20:
        ret_20d = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20]
        points["return_20d_pct"] = round(ret_20d * 100, 2)
        if ret_20d > 0.10:
            scores.append(0.6)
        elif ret_20d > 0.03:
            scores.append(0.3)
        elif ret_20d > -0.03:
            scores.append(0.0)
        elif ret_20d > -0.10:
            scores.append(-0.3)
        else:
            scores.append(-0.6)


def _score_volume_confirmation(data: StockData, scores: list[float], points: dict) -> None:
    """Score whether volume confirms the price trend."""
    if data.history_30d is None or len(data.history_30d) < 20:
        return
    if "Volume" not in data.history_30d.columns:
        return

    vol = data.history_30d["Volume"]
    close = data.history_30d["Close"]

    avg_vol_20d = vol.iloc[-20:].mean()
    recent_vol = vol.iloc[-5:].mean()
    if avg_vol_20d <= 0:
        return

    vol_ratio = recent_vol / avg_vol_20d
    points["volume_ratio_5d_vs_20d"] = round(vol_ratio, 2)

    ret_5d = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5]

    # Volume confirms trend
    if ret_5d > 0.01 and vol_ratio > 1.3:
        points["volume_confirmation"] = "bullish_confirmed"
        scores.append(0.4)
    elif ret_5d < -0.01 and vol_ratio > 1.3:
        points["volume_confirmation"] = "bearish_confirmed"
        scores.append(-0.4)
    elif ret_5d > 0.01 and vol_ratio < 0.7:
        points["volume_confirmation"] = "bullish_weak"
        scores.append(-0.1)  # Price up on low volume = weak
    elif ret_5d < -0.01 and vol_ratio < 0.7:
        points["volume_confirmation"] = "bearish_weak"
        scores.append(0.1)  # Price down on low volume = less conviction


def _score_beta_momentum(data: StockData, scores: list[float], points: dict) -> None:
    """Factor in beta for momentum context."""
    if data.beta <= 0:
        return
    points["beta"] = round(data.beta, 2)
    # High beta stocks have amplified momentum — flag but don't score directionally


def _build_reasoning(direction: str, points: dict) -> str:
    parts = []
    if "return_5d_pct" in points:
        parts.append(f"5d {points['return_5d_pct']:+.1f}%")
    if "return_20d_pct" in points:
        parts.append(f"20d {points['return_20d_pct']:+.1f}%")
    conf = points.get("volume_confirmation")
    if conf:
        parts.append(f"vol {conf}")
    detail = ", ".join(parts) if parts else "limited data"
    return f"Momentum {direction}: {detail}"
