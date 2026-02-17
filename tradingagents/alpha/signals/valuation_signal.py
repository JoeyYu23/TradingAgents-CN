"""Valuation signal extraction from PE, PB, analyst targets."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .signal_types import Signal

if TYPE_CHECKING:
    from ..data_collectors.yfinance_collector import StockData


def extract(data: StockData) -> Signal:
    """Extract valuation signal from fundamentals and analyst targets."""
    points: dict = {}
    scores: list[float] = []

    confidence = _assess_confidence(data)

    _score_pe(data, scores, points)
    _score_analyst_target(data, scores, points)
    _score_pb(data, scores, points)
    _score_ev_ebitda(data, scores, points)

    if not scores:
        return Signal(
            name="valuation",
            direction="neutral",
            strength=0.0,
            confidence=0.0,
            data_points=points,
            reasoning="Insufficient valuation data",
        )

    avg_score = sum(scores) / len(scores)
    direction = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
    strength = min(abs(avg_score), 1.0)

    return Signal(
        name="valuation",
        direction=direction,
        strength=round(strength, 2),
        confidence=round(confidence, 2),
        data_points=points,
        reasoning=_build_reasoning(data, direction, points),
    )


def _assess_confidence(data: StockData) -> float:
    score = 0.0
    if data.pe_trailing > 0 or data.pe_forward > 0:
        score += 0.3
    if data.analyst_target_mean > 0:
        score += 0.3
    if data.pb > 0:
        score += 0.2
    if data.ev_ebitda > 0:
        score += 0.2
    return score


def _score_pe(data: StockData, scores: list[float], points: dict) -> None:
    """Score PE ratio: high PE = expensive = bearish, low PE = cheap = bullish."""
    pe = data.pe_forward if data.pe_forward > 0 else data.pe_trailing
    if pe <= 0:
        return
    points["pe_used"] = round(pe, 1)
    points["pe_type"] = "forward" if data.pe_forward > 0 else "trailing"

    # Compare forward vs trailing for growth expectations
    if data.pe_forward > 0 and data.pe_trailing > 0:
        pe_contraction = data.pe_forward / data.pe_trailing
        points["pe_fwd_vs_trail"] = round(pe_contraction, 2)
        if pe_contraction < 0.7:
            scores.append(0.4)  # Strong earnings growth expected
        elif pe_contraction < 0.9:
            scores.append(0.2)

    # Absolute PE assessment (sector-agnostic rough thresholds)
    if pe > 60:
        scores.append(-0.6)
    elif pe > 40:
        scores.append(-0.3)
    elif pe > 25:
        scores.append(0.0)
    elif pe > 15:
        scores.append(0.2)
    else:
        scores.append(0.4)


def _score_analyst_target(data: StockData, scores: list[float], points: dict) -> None:
    """Score analyst consensus target vs current price."""
    if data.analyst_target_mean <= 0 or data.current_price <= 0:
        return
    upside = (data.analyst_target_mean - data.current_price) / data.current_price
    points["analyst_target_mean"] = data.analyst_target_mean
    points["analyst_upside_pct"] = round(upside * 100, 1)
    points["analyst_count"] = data.analyst_count

    # Discount analyst targets if few analysts cover
    weight = 1.0 if data.analyst_count >= 10 else 0.6

    if upside > 0.30:
        scores.append(0.7 * weight)
    elif upside > 0.15:
        scores.append(0.4 * weight)
    elif upside > 0.05:
        scores.append(0.2 * weight)
    elif upside > -0.10:
        scores.append(0.0)
    elif upside > -0.20:
        scores.append(-0.3 * weight)
    else:
        scores.append(-0.6 * weight)


def _score_pb(data: StockData, scores: list[float], points: dict) -> None:
    """Score price-to-book ratio."""
    if data.pb <= 0:
        return
    points["pb"] = round(data.pb, 2)
    if data.pb > 15:
        scores.append(-0.3)
    elif data.pb > 8:
        scores.append(-0.1)
    elif data.pb < 1.5:
        scores.append(0.3)


def _score_ev_ebitda(data: StockData, scores: list[float], points: dict) -> None:
    """Score EV/EBITDA ratio."""
    if data.ev_ebitda <= 0:
        return
    points["ev_ebitda"] = round(data.ev_ebitda, 1)
    if data.ev_ebitda > 30:
        scores.append(-0.3)
    elif data.ev_ebitda > 20:
        scores.append(-0.1)
    elif data.ev_ebitda < 10:
        scores.append(0.3)


def _build_reasoning(data: StockData, direction: str, points: dict) -> str:
    parts = []
    if "pe_used" in points:
        parts.append(f"PE {points['pe_used']:.0f}")
    if "analyst_upside_pct" in points:
        parts.append(f"analyst target {points['analyst_upside_pct']:+.0f}%")
    detail = ", ".join(parts) if parts else "limited data"
    return f"Valuation {direction}: {detail}"
