"""Data collectors for the Alpha Contradiction Engine.

Provides market data collection from yfinance (stock-level),
macro indicators (VIX, treasuries, commodities, sectors),
and FRED economic data (interest rates, CPI, unemployment).
"""

from .yfinance_collector import StockData, collect as collect_stock
from .macro_collector import MacroData, collect as collect_macro
from .fred_collector import FredData, collect as collect_fred
from .news_collector import NewsData, collect as collect_news

__all__ = [
    "StockData",
    "MacroData",
    "FredData",
    "NewsData",
    "collect_stock",
    "collect_macro",
    "collect_fred",
    "collect_news",
]
