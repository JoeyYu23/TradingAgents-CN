"""Runner entry point for the Alpha Contradiction Engine.

Usage:
    python -m tradingagents.alpha.runner NVDA
    python -m tradingagents.alpha.runner AAPL MSFT GOOGL
"""

from __future__ import annotations

import argparse
import logging
import sys

from .contradiction_engine import ContradictionEngine
from .data_collectors import fred_collector, macro_collector, news_collector, yfinance_collector
from .report_generator import ReportGenerator
from .signals import (
    Signal,
    extract_earnings,
    extract_insider,
    extract_macro,
    extract_momentum,
    extract_news,
    extract_options,
    extract_technical,
    extract_valuation,
)

logger = logging.getLogger(__name__)


def _extract_all_signals(
    stock_data: yfinance_collector.StockData,
    macro_data: macro_collector.MacroData,
    news_data: news_collector.NewsData,
) -> list[Signal]:
    """Run all signal extractors and return the results."""
    signals: list[Signal] = []

    extractors = [
        ("technical", lambda: extract_technical(stock_data)),
        ("valuation", lambda: extract_valuation(stock_data)),
        ("insider_activity", lambda: extract_insider(stock_data)),
        ("earnings", lambda: extract_earnings(stock_data)),
        ("options", lambda: extract_options(stock_data)),
        ("momentum", lambda: extract_momentum(stock_data)),
        ("macro", lambda: extract_macro(stock_data, macro_data)),
        ("news_sentiment", lambda: extract_news(news_data)),
    ]

    for name, fn in extractors:
        try:
            signals.append(fn())
        except Exception as exc:
            logger.warning("Signal extractor %s failed: %s", name, exc)
            signals.append(Signal(
                name=name,
                direction="neutral",
                strength=0.0,
                confidence=0.0,
                reasoning=f"Extraction failed: {exc}",
            ))

    return signals


def _get_stock_sector(stock_data: yfinance_collector.StockData) -> str:
    """Attempt to determine the stock's sector from yfinance info."""
    try:
        import yfinance as yf
        info = yf.Ticker(stock_data.ticker).info
        return str(info.get("sector", ""))
    except Exception:
        return ""


def analyze(ticker: str) -> dict:
    """Full pipeline: ticker -> structured analysis.

    Returns a dict with keys: signals, contradictions, score, report.
    """
    logger.info("Analyzing %s ...", ticker)

    # Phase 1: Collect data
    stock_data = yfinance_collector.collect(ticker)
    sector = _get_stock_sector(stock_data)
    macro_data = macro_collector.collect(stock_sector=sector)
    fred_data = fred_collector.collect()
    news_data = news_collector.collect(ticker)

    # Phase 2: Extract signals
    signals = _extract_all_signals(stock_data, macro_data, news_data)

    # Phase 3: Detect contradictions
    engine = ContradictionEngine()
    contradictions = engine.detect(signals)
    score = engine.score_overall(signals, contradictions)

    # Phase 4: Generate report
    report = ReportGenerator().generate(
        ticker, stock_data, signals, contradictions, score
    )

    return {
        "ticker": ticker,
        "signals": signals,
        "contradictions": contradictions,
        "score": score,
        "report": report,
        "fred_data": fred_data,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Alpha Contradiction Engine - data-driven stock analysis"
    )
    parser.add_argument(
        "tickers",
        nargs="+",
        help="One or more stock ticker symbols (e.g. NVDA AAPL)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    for ticker in args.tickers:
        result = analyze(ticker.upper())
        sys.stdout.write(result["report"])
        sys.stdout.write("\n\n" + "=" * 72 + "\n\n")


if __name__ == "__main__":
    main()
