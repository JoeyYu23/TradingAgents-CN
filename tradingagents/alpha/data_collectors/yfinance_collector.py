"""Stock data collector using yfinance for the Alpha Contradiction Engine.

Fetches price history, analyst consensus, insider activity, options data,
short interest, earnings, and valuation metrics for a single ticker.
No LLM calls -- pure data retrieval and calculation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class StockData:
    """Aggregated stock-level data snapshot for contradiction analysis."""

    ticker: str = ""
    current_price: float = 0.0

    # Price & Technical
    history_30d: pd.DataFrame = field(default_factory=pd.DataFrame)
    ma_50: float = 0.0
    ma_200: float = 0.0
    rsi_14: float = 0.0

    # Analyst Consensus
    analyst_target_low: float = 0.0
    analyst_target_mean: float = 0.0
    analyst_target_high: float = 0.0
    analyst_count: int = 0
    analyst_rating: str = ""
    recent_upgrades: list = field(default_factory=list)

    # Insider Activity
    insider_transactions: list = field(default_factory=list)
    insider_net_shares: int = 0
    insiders_pct_held: float = 0.0
    institutions_pct_held: float = 0.0

    # Options
    put_call_ratio_oi: float = 0.0
    atm_call_iv: float = 0.0
    atm_put_iv: float = 0.0
    iv_skew: float = 0.0

    # Short Interest
    short_pct_float: float = 0.0
    short_ratio_days: float = 0.0

    # Earnings
    next_earnings_date: Optional[date] = None
    days_to_earnings: Optional[int] = None
    last_4_surprises: list = field(default_factory=list)

    # Valuation
    pe_trailing: float = 0.0
    pe_forward: float = 0.0
    pb: float = 0.0
    ev_ebitda: float = 0.0
    beta: float = 0.0
    week52_high: float = 0.0
    week52_low: float = 0.0
    pct_from_52w_high: float = 0.0


# ---------------------------------------------------------------------------
# Helper: RSI calculation (standard 14-period Wilder's EMA)
# ---------------------------------------------------------------------------

def _compute_rsi(close: pd.Series, period: int = 14) -> float:
    """Return the latest RSI value from a price series."""
    if len(close) < period + 1:
        return 0.0
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    last_avg_loss = avg_loss.iloc[-1]
    if last_avg_loss == 0:
        return 100.0
    rs = avg_gain.iloc[-1] / last_avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


# ---------------------------------------------------------------------------
# Helper: moving averages
# ---------------------------------------------------------------------------

def _moving_average(close: pd.Series, window: int) -> float:
    """Return the latest simple moving average value."""
    if len(close) < window:
        return 0.0
    return float(close.rolling(window=window).mean().iloc[-1])


# ---------------------------------------------------------------------------
# Section collectors -- each wrapped in try/except for resilience
# ---------------------------------------------------------------------------

def _collect_price_and_technical(
    ticker_obj: yf.Ticker, data: StockData
) -> None:
    """Fetch 30-day OHLCV history and compute MA / RSI."""
    try:
        info = ticker_obj.info
        data.current_price = float(info.get("currentPrice", 0) or 0)
    except Exception:
        logger.debug("Failed to get current price from info for %s", data.ticker)

    try:
        # Fetch enough history for MA-200 + RSI warmup
        hist = ticker_obj.history(period="1y")
        if hist.empty:
            return
        # Store 30-day slice
        data.history_30d = hist.tail(30).copy()
        close = hist["Close"]
        data.ma_50 = _moving_average(close, 50)
        data.ma_200 = _moving_average(close, 200)
        data.rsi_14 = _compute_rsi(close, 14)
        if data.current_price == 0.0 and not close.empty:
            data.current_price = float(close.iloc[-1])
    except Exception as exc:
        logger.debug("Price/technical fetch failed for %s: %s", data.ticker, exc)


def _collect_analyst(ticker_obj: yf.Ticker, data: StockData) -> None:
    """Fetch analyst consensus targets and rating changes."""
    try:
        info = ticker_obj.info
        data.analyst_target_low = float(info.get("targetLowPrice", 0) or 0)
        data.analyst_target_mean = float(info.get("targetMeanPrice", 0) or 0)
        data.analyst_target_high = float(info.get("targetHighPrice", 0) or 0)
        data.analyst_count = int(info.get("numberOfAnalystOpinions", 0) or 0)
        data.analyst_rating = str(info.get("recommendationKey", "") or "")
    except Exception as exc:
        logger.debug("Analyst info fetch failed for %s: %s", data.ticker, exc)

    try:
        upgrades = ticker_obj.upgrades_downgrades
        if upgrades is not None and not upgrades.empty:
            recent = upgrades.head(5)
            data.recent_upgrades = recent.reset_index().to_dict("records")
    except Exception as exc:
        logger.debug("Upgrades fetch failed for %s: %s", data.ticker, exc)


def _collect_insider(ticker_obj: yf.Ticker, data: StockData) -> None:
    """Fetch insider transactions and ownership percentages."""
    try:
        txns = ticker_obj.insider_transactions
        if txns is not None and not txns.empty:
            # Filter to last 6 months
            six_months_ago = datetime.now() - timedelta(days=180)
            if "Start Date" in txns.columns:
                txns_dt = txns.copy()
                txns_dt["Start Date"] = pd.to_datetime(
                    txns_dt["Start Date"], errors="coerce"
                )
                txns_dt = txns_dt[txns_dt["Start Date"] >= six_months_ago]
                data.insider_transactions = txns_dt.to_dict("records")
            else:
                data.insider_transactions = txns.head(20).to_dict("records")

            # Net shares: positive for buys, negative for sales
            if "Shares" in txns.columns and "Text" in txns.columns:
                net = 0
                for _, row in txns.iterrows():
                    shares = int(row.get("Shares", 0) or 0)
                    text = str(row.get("Text", "")).lower()
                    if "sale" in text or "sell" in text:
                        net -= shares
                    elif "purchase" in text or "buy" in text:
                        net += shares
                data.insider_net_shares = net
    except Exception as exc:
        logger.debug("Insider transactions fetch failed for %s: %s", data.ticker, exc)

    try:
        info = ticker_obj.info
        data.insiders_pct_held = float(
            info.get("heldPercentInsiders", 0) or 0
        )
        data.institutions_pct_held = float(
            info.get("heldPercentInstitutions", 0) or 0
        )
    except Exception as exc:
        logger.debug("Ownership pct fetch failed for %s: %s", data.ticker, exc)


def _collect_options(ticker_obj: yf.Ticker, data: StockData) -> None:
    """Compute put/call OI ratio and ATM implied volatility."""
    try:
        expirations = ticker_obj.options
        if not expirations:
            return
        # Use nearest 3 expirations (or fewer if not available)
        nearest = expirations[:3]
        total_call_oi = 0
        total_put_oi = 0
        atm_call_ivs: list[float] = []
        atm_put_ivs: list[float] = []
        current = data.current_price

        for exp in nearest:
            chain = ticker_obj.option_chain(exp)
            calls = chain.calls
            puts = chain.puts

            total_call_oi += int(calls["openInterest"].fillna(0).sum())
            total_put_oi += int(puts["openInterest"].fillna(0).sum())

            if current > 0:
                _extract_atm_iv(calls, puts, current, atm_call_ivs, atm_put_ivs)

        if total_call_oi > 0:
            data.put_call_ratio_oi = round(total_put_oi / total_call_oi, 4)

        if atm_call_ivs:
            data.atm_call_iv = round(float(np.mean(atm_call_ivs)), 4)
        if atm_put_ivs:
            data.atm_put_iv = round(float(np.mean(atm_put_ivs)), 4)
        data.iv_skew = round(data.atm_put_iv - data.atm_call_iv, 4)
    except Exception as exc:
        logger.debug("Options fetch failed for %s: %s", data.ticker, exc)


def _extract_atm_iv(
    calls: pd.DataFrame,
    puts: pd.DataFrame,
    current_price: float,
    call_ivs: list[float],
    put_ivs: list[float],
) -> None:
    """Find ATM strike implied volatilities and append to accumulator lists."""
    if calls.empty or puts.empty:
        return
    # ATM = strike closest to current price
    call_atm_idx = (calls["strike"] - current_price).abs().idxmin()
    put_atm_idx = (puts["strike"] - current_price).abs().idxmin()

    call_iv = calls.loc[call_atm_idx, "impliedVolatility"]
    put_iv = puts.loc[put_atm_idx, "impliedVolatility"]

    if pd.notna(call_iv) and call_iv > 0:
        call_ivs.append(float(call_iv))
    if pd.notna(put_iv) and put_iv > 0:
        put_ivs.append(float(put_iv))


def _collect_short_interest(ticker_obj: yf.Ticker, data: StockData) -> None:
    """Fetch short interest metrics from ticker.info."""
    try:
        info = ticker_obj.info
        data.short_pct_float = float(
            info.get("shortPercentOfFloat", 0) or 0
        )
        data.short_ratio_days = float(info.get("shortRatio", 0) or 0)
    except Exception as exc:
        logger.debug("Short interest fetch failed for %s: %s", data.ticker, exc)


def _collect_earnings(ticker_obj: yf.Ticker, data: StockData) -> None:
    """Fetch next earnings date, days-to-earnings, and surprise history."""
    try:
        cal = ticker_obj.calendar
        if cal is not None:
            # calendar can be a dict or DataFrame depending on yfinance version
            if isinstance(cal, dict):
                earnings_date_raw = cal.get("Earnings Date")
            else:
                earnings_date_raw = (
                    cal.get("Earnings Date") if "Earnings Date" in cal else None
                )

            if earnings_date_raw is not None:
                if isinstance(earnings_date_raw, list) and earnings_date_raw:
                    ed = earnings_date_raw[0]
                else:
                    ed = earnings_date_raw
                if hasattr(ed, "date"):
                    data.next_earnings_date = ed.date()
                elif isinstance(ed, date):
                    data.next_earnings_date = ed
                if data.next_earnings_date:
                    delta = data.next_earnings_date - date.today()
                    data.days_to_earnings = delta.days
    except Exception as exc:
        logger.debug("Earnings calendar fetch failed for %s: %s", data.ticker, exc)

    try:
        earnings = ticker_obj.earnings_history
        if earnings is not None and not earnings.empty:
            surprises = []
            for _, row in earnings.tail(4).iterrows():
                surprises.append(
                    {
                        "estimate": row.get("epsEstimate"),
                        "actual": row.get("epsActual"),
                        "surprise_pct": row.get("surprisePercent"),
                    }
                )
            data.last_4_surprises = surprises
    except Exception as exc:
        logger.debug("Earnings history fetch failed for %s: %s", data.ticker, exc)


def _collect_valuation(ticker_obj: yf.Ticker, data: StockData) -> None:
    """Fetch valuation multiples and 52-week range from ticker.info."""
    try:
        info = ticker_obj.info
        data.pe_trailing = float(info.get("trailingPE", 0) or 0)
        data.pe_forward = float(info.get("forwardPE", 0) or 0)
        data.pb = float(info.get("priceToBook", 0) or 0)
        data.ev_ebitda = float(info.get("enterpriseToEbitda", 0) or 0)
        data.beta = float(info.get("beta", 0) or 0)
        data.week52_high = float(info.get("fiftyTwoWeekHigh", 0) or 0)
        data.week52_low = float(info.get("fiftyTwoWeekLow", 0) or 0)
        if data.week52_high > 0 and data.current_price > 0:
            data.pct_from_52w_high = round(
                (data.current_price - data.week52_high) / data.week52_high * 100,
                2,
            )
    except Exception as exc:
        logger.debug("Valuation fetch failed for %s: %s", data.ticker, exc)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def collect(ticker: str) -> StockData:
    """Collect all stock data for a given ticker symbol.

    Each data section is independently wrapped in try/except so that
    partial failures do not prevent other sections from populating.
    """
    data = StockData(ticker=ticker.upper())
    ticker_obj = yf.Ticker(ticker.upper())

    _collect_price_and_technical(ticker_obj, data)
    _collect_analyst(ticker_obj, data)
    _collect_insider(ticker_obj, data)
    _collect_options(ticker_obj, data)
    _collect_short_interest(ticker_obj, data)
    _collect_earnings(ticker_obj, data)
    _collect_valuation(ticker_obj, data)

    return data
