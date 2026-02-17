"""Macro environment signal extraction from VIX, yields, and sector data."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .signal_types import Signal

if TYPE_CHECKING:
    from ..data_collectors.macro_collector import MacroData
    from ..data_collectors.yfinance_collector import StockData


def extract(data: StockData, macro: MacroData) -> Signal:
    """Extract macro environment signal from market-wide indicators."""
    points: dict = {}
    scores: list[float] = []

    confidence = _assess_confidence(macro)

    _score_vix(macro, scores, points)
    _score_yield_curve(macro, scores, points)
    _score_sector_rotation(macro, scores, points)

    if not scores:
        return Signal(
            name="macro",
            direction="neutral",
            strength=0.0,
            confidence=0.0,
            data_points=points,
            reasoning="Insufficient macro data",
        )

    avg_score = sum(scores) / len(scores)
    direction = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
    strength = min(abs(avg_score), 1.0)

    return Signal(
        name="macro",
        direction=direction,
        strength=round(strength, 2),
        confidence=round(confidence, 2),
        data_points=points,
        reasoning=_build_reasoning(direction, points),
    )


def _assess_confidence(macro: MacroData) -> float:
    score = 0.0
    if macro.vix > 0:
        score += 0.3
    if macro.treasury_10y > 0:
        score += 0.2
    if macro.sector_returns_1m:
        score += 0.3
    if macro.gold_price > 0:
        score += 0.1
    if macro.oil_price > 0:
        score += 0.1
    return min(score, 1.0)


def _score_vix(macro: MacroData, scores: list[float], points: dict) -> None:
    """Score VIX level and trend."""
    if macro.vix <= 0:
        return
    points["vix"] = round(macro.vix, 1)
    points["vix_5d_change"] = round(macro.vix_5d_change, 1)

    # VIX level
    if macro.vix > 30:
        scores.append(-0.6)  # High fear
        points["vix_regime"] = "fear"
    elif macro.vix > 20:
        scores.append(-0.2)  # Elevated
        points["vix_regime"] = "elevated"
    elif macro.vix < 13:
        scores.append(0.3)  # Complacent (can be contrarian bearish)
        points["vix_regime"] = "complacent"
    else:
        points["vix_regime"] = "normal"

    # VIX trend
    if macro.vix_5d_change > 20:
        scores.append(-0.4)  # VIX spiking
    elif macro.vix_5d_change < -15:
        scores.append(0.3)  # VIX collapsing (risk-on)


def _score_yield_curve(macro: MacroData, scores: list[float], points: dict) -> None:
    """Score yield curve shape and rates environment."""
    if macro.treasury_10y <= 0:
        return
    points["treasury_10y"] = round(macro.treasury_10y, 2)

    if macro.treasury_2y > 0:
        points["treasury_2y"] = round(macro.treasury_2y, 2)
        spread = macro.yield_spread_10y2y
        points["yield_spread_10y2y"] = round(spread, 2)

        # Inverted yield curve = recession signal
        if spread < -0.5:
            scores.append(-0.5)
            points["yield_curve"] = "deeply_inverted"
        elif spread < 0:
            scores.append(-0.3)
            points["yield_curve"] = "inverted"
        elif spread > 1.0:
            scores.append(0.2)
            points["yield_curve"] = "steep"
        else:
            points["yield_curve"] = "normal"


def _score_sector_rotation(macro: MacroData, scores: list[float], points: dict) -> None:
    """Score sector relative performance."""
    if not macro.sector_returns_1m:
        return

    points["sector_returns_1m"] = {
        k: round(v, 2) for k, v in macro.sector_returns_1m.items()
    }

    if macro.stock_sector and macro.sector_vs_spy != 0:
        points["stock_sector"] = macro.stock_sector
        points["sector_vs_spy"] = round(macro.sector_vs_spy, 2)

        # Sector outperforming SPY = tailwind
        if macro.sector_vs_spy > 3:
            scores.append(0.4)
        elif macro.sector_vs_spy > 1:
            scores.append(0.2)
        elif macro.sector_vs_spy < -3:
            scores.append(-0.4)
        elif macro.sector_vs_spy < -1:
            scores.append(-0.2)

    # Defensive vs cyclical rotation
    defensive = ["XLU", "XLP", "XLV"]
    cyclical = ["XLK", "XLY", "XLI", "XLF"]
    def_avg = _avg_return(macro.sector_returns_1m, defensive)
    cyc_avg = _avg_return(macro.sector_returns_1m, cyclical)

    if def_avg is not None and cyc_avg is not None:
        rotation = cyc_avg - def_avg
        points["cyclical_vs_defensive"] = round(rotation, 2)
        if rotation > 3:
            scores.append(0.3)  # Risk-on rotation
        elif rotation < -3:
            scores.append(-0.3)  # Defensive rotation


def _avg_return(returns: dict, tickers: list[str]) -> float | None:
    vals = [returns[t] for t in tickers if t in returns]
    return sum(vals) / len(vals) if vals else None


def _build_reasoning(direction: str, points: dict) -> str:
    parts = []
    if "vix" in points:
        parts.append(f"VIX {points['vix']:.0f}")
    if "vix_regime" in points:
        parts.append(points["vix_regime"])
    if "yield_curve" in points:
        parts.append(f"curve {points['yield_curve']}")
    if "sector_vs_spy" in points:
        parts.append(f"sector vs SPY {points['sector_vs_spy']:+.1f}%")
    detail = ", ".join(parts) if parts else "limited data"
    return f"Macro {direction}: {detail}"
