"""Macro data collector using yfinance for the Alpha Contradiction Engine.

Fetches VIX, treasury yields, commodities, dollar index, and sector
ETF performance. No LLM calls -- pure data retrieval and calculation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# Sector ETF ticker -> human-readable name
SECTOR_ETFS: dict[str, str] = {
    "XLK": "Technology",
    "XLF": "Financials",
    "XLE": "Energy",
    "XLV": "Healthcare",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLI": "Industrials",
    "XLB": "Materials",
    "XLRE": "Real Estate",
    "XLC": "Communication Services",
    "XLU": "Utilities",
}

# Map common yfinance sector strings to their ETF ticker
_SECTOR_TO_ETF: dict[str, str] = {
    "technology": "XLK",
    "financial services": "XLF",
    "financials": "XLF",
    "energy": "XLE",
    "healthcare": "XLV",
    "consumer cyclical": "XLY",
    "consumer discretionary": "XLY",
    "consumer defensive": "XLP",
    "consumer staples": "XLP",
    "industrials": "XLI",
    "basic materials": "XLB",
    "materials": "XLB",
    "real estate": "XLRE",
    "communication services": "XLC",
    "utilities": "XLU",
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class MacroData:
    """Macro-level market data snapshot for contradiction analysis."""

    vix: float = 0.0
    vix_5d_change: float = 0.0
    treasury_10y: float = 0.0
    treasury_2y: float = 0.0
    yield_spread_10y2y: float = 0.0
    gold_price: float = 0.0
    oil_price: float = 0.0
    dollar_index: float = 0.0
    sector_returns_1m: dict = field(default_factory=dict)
    stock_sector: str = ""
    sector_vs_spy: float = 0.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_last_close(ticker_symbol: str, period: str = "5d") -> float:
    """Return the last closing price for a ticker, or 0.0 on failure."""
    try:
        hist = yf.Ticker(ticker_symbol).history(period=period)
        if hist.empty:
            return 0.0
        return float(hist["Close"].iloc[-1])
    except Exception as exc:
        logger.debug("Failed to fetch %s: %s", ticker_symbol, exc)
        return 0.0


def _safe_pct_change(ticker_symbol: str, period: str = "10d") -> float:
    """Return percentage change over the given period, or 0.0."""
    try:
        hist = yf.Ticker(ticker_symbol).history(period=period)
        if hist.empty or len(hist) < 2:
            return 0.0
        close = hist["Close"]
        # Use first and last available close in the window
        return float((close.iloc[-1] / close.iloc[0] - 1.0) * 100.0)
    except Exception as exc:
        logger.debug("Failed pct change for %s: %s", ticker_symbol, exc)
        return 0.0


def _one_month_return(ticker_symbol: str) -> float:
    """Compute ~1-month return (22 trading days) for a ticker."""
    try:
        hist = yf.Ticker(ticker_symbol).history(period="1mo")
        if hist.empty or len(hist) < 2:
            return 0.0
        close = hist["Close"]
        return float((close.iloc[-1] / close.iloc[0] - 1.0) * 100.0)
    except Exception as exc:
        logger.debug("Failed 1m return for %s: %s", ticker_symbol, exc)
        return 0.0


def _resolve_sector_etf(stock_sector: str) -> str:
    """Map a stock sector name to its corresponding sector ETF ticker."""
    normalized = stock_sector.strip().lower()
    return _SECTOR_TO_ETF.get(normalized, "")


# ---------------------------------------------------------------------------
# Section collectors
# ---------------------------------------------------------------------------

def _collect_vix(data: MacroData) -> None:
    """Fetch VIX level and 5-day change."""
    try:
        vix_hist = yf.Ticker("^VIX").history(period="10d")
        if vix_hist.empty:
            return
        close = vix_hist["Close"]
        data.vix = round(float(close.iloc[-1]), 2)
        if len(close) >= 6:
            data.vix_5d_change = round(
                float((close.iloc[-1] / close.iloc[-6] - 1.0) * 100.0), 2
            )
    except Exception as exc:
        logger.debug("VIX fetch failed: %s", exc)


def _collect_treasuries(data: MacroData) -> None:
    """Fetch 10Y and 2Y treasury yields and compute spread."""
    try:
        data.treasury_10y = round(_safe_last_close("^TNX"), 3)
    except Exception as exc:
        logger.debug("10Y treasury fetch failed: %s", exc)

    try:
        # 2Y yield: try "2YY=F" futures first, fall back to "^IRX" (3-month)
        val = _safe_last_close("2YY=F")
        if val == 0.0:
            val = _safe_last_close("^IRX")
        data.treasury_2y = round(val, 3)
    except Exception as exc:
        logger.debug("2Y treasury fetch failed: %s", exc)

    if data.treasury_10y > 0 and data.treasury_2y > 0:
        data.yield_spread_10y2y = round(
            data.treasury_10y - data.treasury_2y, 3
        )


def _collect_commodities(data: MacroData) -> None:
    """Fetch gold, oil, and dollar index."""
    try:
        data.gold_price = round(_safe_last_close("GC=F"), 2)
    except Exception as exc:
        logger.debug("Gold fetch failed: %s", exc)

    try:
        data.oil_price = round(_safe_last_close("CL=F"), 2)
    except Exception as exc:
        logger.debug("Oil fetch failed: %s", exc)

    try:
        data.dollar_index = round(_safe_last_close("DX-Y.NYB"), 2)
    except Exception as exc:
        logger.debug("Dollar index fetch failed: %s", exc)


def _collect_sectors(data: MacroData, stock_sector: str) -> None:
    """Compute 1-month returns for each sector ETF and compare to SPY."""
    try:
        spy_return = _one_month_return("SPY")
        for etf_ticker, sector_name in SECTOR_ETFS.items():
            ret = _one_month_return(etf_ticker)
            data.sector_returns_1m[sector_name] = round(ret, 2)
    except Exception as exc:
        logger.debug("Sector return calculation failed: %s", exc)
        spy_return = 0.0

    # Determine the stock's sector performance vs SPY
    data.stock_sector = stock_sector
    etf_ticker = _resolve_sector_etf(stock_sector)
    if etf_ticker:
        sector_return = data.sector_returns_1m.get(
            SECTOR_ETFS.get(etf_ticker, ""), 0.0
        )
        data.sector_vs_spy = round(sector_return - spy_return, 2)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def collect(stock_sector: str = "") -> MacroData:
    """Collect macro market data.

    Args:
        stock_sector: The sector of the stock being analyzed (e.g.
            "Technology", "Healthcare"). Used to compute relative
            sector performance vs SPY.

    Returns:
        MacroData with VIX, treasuries, commodities, and sector metrics.
    """
    data = MacroData()

    _collect_vix(data)
    _collect_treasuries(data)
    _collect_commodities(data)
    _collect_sectors(data, stock_sector)

    return data
