"""Contradiction detection engine for the Alpha Contradiction Engine.

Compares directional signals pairwise against a rule table, identifies
contradictions, and produces an overall score. No LLM calls.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .signals.signal_types import Signal


@dataclass
class Contradiction:
    """A detected contradiction between two opposing signals."""

    signal_a: Signal
    signal_b: Signal
    severity: Literal["high", "medium", "low"] = "low"
    category: str = ""
    description: str = ""
    historical_resolution: str = ""


# Each rule: (signal_a_name, signal_b_name, min_strength, category, resolution_note)
_CONTRADICTION_RULES: list[tuple[str, str, float, str, str]] = [
    (
        "insider_activity", "valuation", 0.3, "smart_money_divergence",
        "Insider activity is more predictive than analyst ratings",
    ),
    (
        "technical", "valuation", 0.4, "technical_fundamental_divergence",
        "Fundamentals tend to dominate over 3-6 months; technicals over 1-4 weeks",
    ),
    (
        "options", "momentum", 0.3, "flow_vs_trend",
        "Options flow often leads momentum by 1-2 weeks",
    ),
    (
        "insider_activity", "momentum", 0.3, "insider_vs_market",
        "Insiders have long-term edge; momentum can persist short-term",
    ),
    (
        "insider_activity", "options", 0.3, "smart_money_vs_flow",
        "Both are informed-trader signals; conflict suggests uncertainty",
    ),
    (
        "macro", "momentum", 0.3, "macro_vs_micro",
        "Macro headwinds eventually override single-stock momentum",
    ),
    (
        "earnings", "valuation", 0.3, "earnings_risk",
        "Upcoming earnings can resolve valuation debates quickly",
    ),
    (
        "news_sentiment", "insider_activity", 0.3, "news_vs_insider",
        "Positive news with insider selling suggests insiders know more than headlines",
    ),
    (
        "news_sentiment", "momentum", 0.3, "news_vs_price",
        "Positive news but declining price suggests market has already priced it in",
    ),
]


class ContradictionEngine:
    """Detect contradictions between signals and produce an overall score."""

    def detect(self, signals: list[Signal]) -> list[Contradiction]:
        """Find all contradictions between provided signals."""
        signal_map = {s.name: s for s in signals}
        contradictions: list[Contradiction] = []

        for name_a, name_b, min_str, category, resolution in _CONTRADICTION_RULES:
            sig_a = signal_map.get(name_a)
            sig_b = signal_map.get(name_b)
            if sig_a is None or sig_b is None:
                continue
            c = _check_pair(sig_a, sig_b, min_str, category, resolution)
            if c is not None:
                contradictions.append(c)

        contradictions.sort(
            key=lambda c: {"high": 0, "medium": 1, "low": 2}[c.severity]
        )
        return contradictions

    def score_overall(
        self, signals: list[Signal], contradictions: list[Contradiction]
    ) -> dict:
        """Produce an overall consensus score from signals and contradictions."""
        bullish = [s for s in signals if s.direction == "bullish" and s.confidence > 0]
        bearish = [s for s in signals if s.direction == "bearish" and s.confidence > 0]
        neutral = [s for s in signals if s.direction == "neutral" and s.confidence > 0]

        bull_weight = sum(s.strength * s.confidence for s in bullish)
        bear_weight = sum(s.strength * s.confidence for s in bearish)
        total_weight = bull_weight + bear_weight
        if total_weight == 0:
            consensus_direction = "neutral"
            consensus_strength = 0.0
        elif bull_weight > bear_weight * 1.3:
            consensus_direction = "bullish"
            consensus_strength = bull_weight / (total_weight + 0.001)
        elif bear_weight > bull_weight * 1.3:
            consensus_direction = "bearish"
            consensus_strength = bear_weight / (total_weight + 0.001)
        else:
            consensus_direction = "mixed"
            consensus_strength = 1.0 - abs(bull_weight - bear_weight) / (total_weight + 0.001)

        high_count = sum(1 for c in contradictions if c.severity == "high")
        med_count = sum(1 for c in contradictions if c.severity == "medium")
        contradiction_severity = _overall_severity(contradictions)
        alpha_potential = _alpha_potential(len(contradictions), high_count, med_count)

        signal_summary = {
            s.name: {
                "direction": s.direction,
                "strength": s.strength,
                "confidence": s.confidence,
                "reasoning": s.reasoning,
            }
            for s in signals
        }

        return {
            "consensus_direction": consensus_direction,
            "consensus_strength": round(consensus_strength, 2),
            "bullish_count": len(bullish),
            "bearish_count": len(bearish),
            "neutral_count": len(neutral),
            "contradiction_count": len(contradictions),
            "contradiction_severity": contradiction_severity,
            "alpha_potential": alpha_potential,
            "key_contradictions": [c.description for c in contradictions[:3]],
            "signal_summary": signal_summary,
        }


def _check_pair(
    sig_a: Signal,
    sig_b: Signal,
    min_strength: float,
    category: str,
    resolution: str,
) -> Contradiction | None:
    """Return a Contradiction if two signals oppose each other above threshold."""
    if sig_a.direction == "neutral" or sig_b.direction == "neutral":
        return None
    if sig_a.direction == sig_b.direction:
        return None

    # Both must have meaningful strength
    if sig_a.strength < min_strength or sig_b.strength < min_strength:
        return None

    # At least one must have reasonable confidence
    if sig_a.confidence < 0.2 and sig_b.confidence < 0.2:
        return None

    combined_strength = (sig_a.strength + sig_b.strength) / 2
    severity = _classify_severity(combined_strength, sig_a.confidence, sig_b.confidence)

    description = (
        f"{sig_a.name.upper()} says {sig_a.direction} ({sig_a.reasoning}) "
        f"but {sig_b.name.upper()} says {sig_b.direction} ({sig_b.reasoning})"
    )

    return Contradiction(
        signal_a=sig_a,
        signal_b=sig_b,
        severity=severity,
        category=category,
        description=description,
        historical_resolution=resolution,
    )


def _classify_severity(
    combined_strength: float, conf_a: float, conf_b: float
) -> Literal["high", "medium", "low"]:
    """Classify contradiction severity based on strength and confidence."""
    avg_conf = (conf_a + conf_b) / 2
    score = combined_strength * 0.6 + avg_conf * 0.4

    if score >= 0.6:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"


def _overall_severity(contradictions: list[Contradiction]) -> str:
    if not contradictions:
        return "none"
    severities = [c.severity for c in contradictions]
    if "high" in severities:
        return "high"
    if "medium" in severities:
        return "medium"
    return "low"


def _alpha_potential(total: int, high: int, medium: int) -> str:
    if high >= 2 or (high >= 1 and medium >= 2):
        return "high"
    if high >= 1 or medium >= 2 or total >= 3:
        return "medium"
    if total >= 1:
        return "low"
    return "none"
