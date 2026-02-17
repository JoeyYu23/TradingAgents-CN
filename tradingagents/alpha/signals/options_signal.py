"""Options flow signal extraction from put/call ratio and implied volatility."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .signal_types import Signal

if TYPE_CHECKING:
    from ..data_collectors.yfinance_collector import StockData


def extract(data: StockData) -> Signal:
    """Extract options signal from P/C ratio, IV skew, and activity."""
    points: dict = {}
    scores: list[float] = []

    confidence = _assess_confidence(data)

    _score_put_call_ratio(data, scores, points)
    _score_iv_skew(data, scores, points)
    _score_iv_level(data, scores, points)

    if not scores:
        return Signal(
            name="options",
            direction="neutral",
            strength=0.0,
            confidence=0.0,
            data_points=points,
            reasoning="No options data available",
        )

    avg_score = sum(scores) / len(scores)
    direction = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
    strength = min(abs(avg_score), 1.0)

    return Signal(
        name="options",
        direction=direction,
        strength=round(strength, 2),
        confidence=round(confidence, 2),
        data_points=points,
        reasoning=_build_reasoning(direction, points),
    )


def _assess_confidence(data: StockData) -> float:
    score = 0.0
    if data.put_call_ratio_oi > 0:
        score += 0.4
    if data.atm_call_iv > 0:
        score += 0.3
    if data.atm_put_iv > 0:
        score += 0.3
    return score


def _score_put_call_ratio(data: StockData, scores: list[float], points: dict) -> None:
    """Score put/call open interest ratio."""
    if data.put_call_ratio_oi <= 0:
        return
    pcr = data.put_call_ratio_oi
    points["put_call_ratio"] = round(pcr, 2)

    # Low P/C ratio = more calls = bullish positioning
    # High P/C ratio = more puts = bearish hedging or sentiment
    if pcr < 0.5:
        scores.append(0.6)  # Heavy call buying
    elif pcr < 0.7:
        scores.append(0.3)  # Moderately bullish
    elif pcr < 1.0:
        scores.append(0.0)  # Neutral
    elif pcr < 1.3:
        scores.append(-0.3)  # Moderately bearish
    else:
        scores.append(-0.6)  # Heavy put buying


def _score_iv_skew(data: StockData, scores: list[float], points: dict) -> None:
    """Score implied volatility skew (put IV - call IV)."""
    if data.iv_skew == 0 and data.atm_call_iv <= 0:
        return
    points["iv_skew"] = round(data.iv_skew, 4)

    # Positive skew = puts more expensive = bearish hedging demand
    # Negative skew = calls more expensive = bullish demand
    if data.iv_skew > 0.10:
        scores.append(-0.5)  # Heavy put demand (fear)
    elif data.iv_skew > 0.03:
        scores.append(-0.2)  # Mild bearish skew
    elif data.iv_skew < -0.05:
        scores.append(0.3)  # Call demand exceeds put demand
    else:
        scores.append(0.0)  # Normal skew


def _score_iv_level(data: StockData, scores: list[float], points: dict) -> None:
    """Score absolute IV level — high IV = uncertainty, not directional per se."""
    avg_iv = 0.0
    count = 0
    if data.atm_call_iv > 0:
        avg_iv += data.atm_call_iv
        count += 1
        points["atm_call_iv"] = round(data.atm_call_iv, 4)
    if data.atm_put_iv > 0:
        avg_iv += data.atm_put_iv
        count += 1
        points["atm_put_iv"] = round(data.atm_put_iv, 4)
    if count == 0:
        return

    avg_iv /= count
    points["avg_iv"] = round(avg_iv, 4)

    # Very high IV signals uncertainty / event risk
    if avg_iv > 0.80:
        points["iv_regime"] = "extreme"
    elif avg_iv > 0.50:
        points["iv_regime"] = "high"
    elif avg_iv > 0.25:
        points["iv_regime"] = "normal"
    else:
        points["iv_regime"] = "low"


def _build_reasoning(direction: str, points: dict) -> str:
    parts = []
    if "put_call_ratio" in points:
        parts.append(f"P/C {points['put_call_ratio']:.2f}")
    if "iv_regime" in points:
        parts.append(f"IV {points['iv_regime']}")
    if "iv_skew" in points:
        skew = points["iv_skew"]
        parts.append(f"skew {skew:+.2f}")
    detail = ", ".join(parts) if parts else "limited data"
    return f"Options {direction}: {detail}"
