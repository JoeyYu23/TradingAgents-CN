"""FRED economic data collector for the Alpha Contradiction Engine.

Provides Federal Reserve economic data (interest rates, CPI, unemployment)
as a graceful fallback -- the system works fine without it.
Requires the optional `fredapi` package and a FRED_API_KEY environment variable.
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# FRED series identifiers
_SERIES_FED_FUNDS = "FEDFUNDS"
_SERIES_CPI = "CPIAUCSL"
_SERIES_UNEMPLOYMENT = "UNRATE"

# Number of months of CPI data needed for YoY calculation (current + 12 prior)
_CPI_LOOKBACK_MONTHS = 13


@dataclass
class FredData:
    """Container for FRED economic indicators.

    Attributes:
        fed_funds_rate: Effective federal funds rate (%).
        cpi_yoy: Consumer Price Index year-over-year change (%).
        unemployment: Civilian unemployment rate (%).
        available: Whether FRED data was successfully fetched.
    """

    fed_funds_rate: Optional[float] = None
    cpi_yoy: Optional[float] = None
    unemployment: Optional[float] = None
    available: bool = False


def collect() -> FredData:
    """Collect latest economic indicators from FRED.

    Returns FredData with available=False if fredapi is not installed
    or FRED_API_KEY is not set. Individual series failures are logged
    and silently skipped -- partial data is still returned.
    """
    try:
        from fredapi import Fred  # noqa: WPS433
    except ImportError:
        logger.debug("fredapi not installed; skipping FRED data collection")
        return FredData(available=False)

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        logger.debug("FRED_API_KEY not set; skipping FRED data collection")
        return FredData(available=False)

    fred = Fred(api_key=api_key)
    result = FredData(available=True)

    result.fed_funds_rate = _fetch_fed_funds_rate(fred)
    result.cpi_yoy = _fetch_cpi_yoy(fred)
    result.unemployment = _fetch_unemployment(fred)

    return result


def _fetch_fed_funds_rate(fred: object) -> Optional[float]:
    """Fetch the latest effective federal funds rate."""
    try:
        series = fred.get_series(_SERIES_FED_FUNDS)  # type: ignore[union-attr]
        if series is not None and len(series) > 0:
            return float(series.iloc[-1])
    except Exception:
        logger.warning("Failed to fetch federal funds rate from FRED", exc_info=True)
    return None


def _fetch_cpi_yoy(fred: object) -> Optional[float]:
    """Calculate CPI year-over-year percentage change from the latest 13 months."""
    try:
        series = fred.get_series(_SERIES_CPI)  # type: ignore[union-attr]
        if series is not None and len(series) >= _CPI_LOOKBACK_MONTHS:
            current = float(series.iloc[-1])
            year_ago = float(series.iloc[-_CPI_LOOKBACK_MONTHS])
            if year_ago != 0:
                return round((current - year_ago) / year_ago * 100, 2)
    except Exception:
        logger.warning("Failed to fetch CPI data from FRED", exc_info=True)
    return None


def _fetch_unemployment(fred: object) -> Optional[float]:
    """Fetch the latest civilian unemployment rate."""
    try:
        series = fred.get_series(_SERIES_UNEMPLOYMENT)  # type: ignore[union-attr]
        if series is not None and len(series) > 0:
            return float(series.iloc[-1])
    except Exception:
        logger.warning("Failed to fetch unemployment rate from FRED", exc_info=True)
    return None
