"""Insider activity signal extraction from Form 4 data."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .signal_types import Signal

if TYPE_CHECKING:
    from ..data_collectors.yfinance_collector import StockData


def extract(data: StockData) -> Signal:
    """Extract insider activity signal from insider transactions."""
    points: dict = {}
    scores: list[float] = []

    confidence = _assess_confidence(data)

    _score_net_shares(data, scores, points)
    _score_transaction_pattern(data, scores, points)
    _score_institutional_ownership(data, scores, points)

    if not scores:
        return Signal(
            name="insider_activity",
            direction="neutral",
            strength=0.0,
            confidence=0.0,
            data_points=points,
            reasoning="No insider transaction data available",
        )

    avg_score = sum(scores) / len(scores)
    direction = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
    strength = min(abs(avg_score), 1.0)

    return Signal(
        name="insider_activity",
        direction=direction,
        strength=round(strength, 2),
        confidence=round(confidence, 2),
        data_points=points,
        reasoning=_build_reasoning(points, direction),
    )


def _assess_confidence(data: StockData) -> float:
    score = 0.0
    if data.insider_transactions:
        score += 0.5
        if len(data.insider_transactions) >= 5:
            score += 0.2
    if data.insiders_pct_held > 0:
        score += 0.15
    if data.institutions_pct_held > 0:
        score += 0.15
    return min(score, 1.0)


def _score_net_shares(data: StockData, scores: list[float], points: dict) -> None:
    """Score net insider buying/selling."""
    points["insider_net_shares"] = data.insider_net_shares
    if data.insider_net_shares == 0 and not data.insider_transactions:
        return

    if data.insider_net_shares > 0:
        scores.append(0.7)  # Net buying is a strong bullish signal
    elif data.insider_net_shares < 0:
        # Net selling is common (compensation-based), bearish only if heavy
        tx_count = len(data.insider_transactions) if data.insider_transactions else 1
        sell_intensity = min(abs(data.insider_net_shares) / max(tx_count, 1) / 10000, 1.0)
        scores.append(-0.3 - 0.4 * sell_intensity)


def _score_transaction_pattern(data: StockData, scores: list[float], points: dict) -> None:
    """Score the pattern of transactions (clusters, C-suite vs others)."""
    if not data.insider_transactions:
        return

    buys = 0
    sells = 0
    for tx in data.insider_transactions:
        text = str(tx).lower()
        if "purchase" in text or "buy" in text:
            buys += 1
        elif "sale" in text or "sell" in text:
            sells += 1

    points["insider_buys"] = buys
    points["insider_sells"] = sells
    total = buys + sells
    if total == 0:
        return

    buy_ratio = buys / total
    points["insider_buy_ratio"] = round(buy_ratio, 2)

    if buy_ratio > 0.6:
        scores.append(0.6)
    elif buy_ratio > 0.3:
        scores.append(0.1)
    elif buy_ratio < 0.1 and sells >= 3:
        scores.append(-0.7)  # All selling, multiple insiders
    elif buy_ratio < 0.2:
        scores.append(-0.4)


def _score_institutional_ownership(data: StockData, scores: list[float], points: dict) -> None:
    """Consider institutional ownership level."""
    if data.institutions_pct_held > 0:
        points["institutions_pct_held"] = round(data.institutions_pct_held * 100, 1)
    if data.insiders_pct_held > 0:
        points["insiders_pct_held"] = round(data.insiders_pct_held * 100, 1)
        # High insider ownership = aligned incentives
        if data.insiders_pct_held > 0.10:
            scores.append(0.2)


def _build_reasoning(points: dict, direction: str) -> str:
    parts = []
    buys = points.get("insider_buys", 0)
    sells = points.get("insider_sells", 0)
    if buys or sells:
        parts.append(f"{buys} buys, {sells} sells")
    net = points.get("insider_net_shares", 0)
    if net != 0:
        parts.append(f"net {'buying' if net > 0 else 'selling'}")
    detail = ", ".join(parts) if parts else "no recent transactions"
    return f"Insider {direction}: {detail}"
