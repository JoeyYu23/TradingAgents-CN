"""Continuous market monitor for the Alpha Contradiction Engine.

Runs the scraper daemon + periodic stock analysis in a single process.
Outputs trading ideas to a log file when contradictions or strong signals
are detected.

Usage:
    python -m tradingagents.alpha.continuous_monitor --duration 120
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .runner import analyze
from .scrapers.jin10_scraper import Jin10Scraper
from .scrapers.store import NewsStore

logger = logging.getLogger(__name__)

_CST = timezone(timedelta(hours=8))

# Stocks to monitor
WATCHLIST = [
    "NVDA", "TSLA", "AAPL", "META", "AMZN",
    "GOOGL", "MSFT", "AMD", "NFLX", "PLTR",
]

# Minimum alpha potential to flag as a trading idea
_ALPHA_THRESHOLDS = {"high", "medium"}


def _format_idea(result: dict, scan_time: str) -> str:
    """Format a trading idea from analysis result."""
    score = result["score"]
    ticker = result["ticker"]
    direction = score["consensus_direction"].upper()
    strength = score["consensus_strength"]
    alpha = score["alpha_potential"].upper()
    contras = score["contradiction_count"]
    severity = score["contradiction_severity"]

    lines = [
        f"{'='*60}",
        f"TRADING IDEA: {ticker}  |  {scan_time}",
        f"{'='*60}",
        f"Direction: {direction} (strength={strength:.2f})",
        f"Alpha Potential: {alpha}",
        f"Contradictions: {contras} ({severity})",
        "",
    ]

    # Signal summary table
    lines.append("Signals:")
    for name, info in score.get("signal_summary", {}).items():
        d = info["direction"].upper()
        s = info["strength"]
        c = info["confidence"]
        lines.append(f"  {name:<20s} {d:<8s} str={s:.2f}  conf={c:.2f}")

    # Key contradictions
    key_cs = score.get("key_contradictions", [])
    if key_cs:
        lines.append("")
        lines.append("Key Contradictions:")
        for kc in key_cs:
            lines.append(f"  -> {kc[:120]}")

    # Actionable interpretation
    lines.append("")
    lines.append(_interpret_idea(score))
    lines.append("")

    return "\n".join(lines)


def _interpret_idea(score: dict) -> str:
    """Generate a brief actionable interpretation."""
    direction = score["consensus_direction"]
    alpha = score["alpha_potential"]
    severity = score.get("contradiction_severity", "none")

    if alpha == "high" and severity == "high":
        return (
            "INTERPRETATION: High-conviction contradictions detected. "
            "Smart money divergence suggests a potential turning point. "
            "Monitor closely for resolution signal before taking position."
        )
    if alpha == "medium":
        return (
            "INTERPRETATION: Moderate contradictions detected. "
            "Signals are split — wait for a catalyst (earnings, macro event) "
            "to break the tie before entering."
        )
    if direction in ("bullish", "bearish") and score["consensus_strength"] > 0.7:
        return (
            f"INTERPRETATION: Strong {direction} consensus with no major "
            f"contradictions. Trend-following setup — consider "
            f"{'long' if direction == 'bullish' else 'short'} exposure."
        )
    return "INTERPRETATION: No clear edge. Wait for stronger signal alignment."


async def _scrape_loop(store: NewsStore, interval: int) -> None:
    """Background scraping loop."""
    jin10 = Jin10Scraper()
    while True:
        try:
            items = await jin10.fetch_latest_with_fallback(max_items=20)
            for item in items:
                store.save(item)
            new_count = len(items)
            total = store.count()
            logger.info("Scraped %d items (total: %d)", new_count, total)
        except Exception as exc:
            logger.error("Scrape cycle failed: %s", exc)
        store.cleanup(days_old=7)
        await asyncio.sleep(interval)


def _scan_all_stocks(
    watchlist: list[str],
    output_path: Path,
) -> int:
    """Analyze all stocks and write trading ideas to the output file."""
    scan_time = datetime.now(tz=_CST).strftime("%Y-%m-%d %H:%M CST")
    ideas_found = 0

    header = (
        f"\n{'#'*60}\n"
        f"  SCAN @ {scan_time}\n"
        f"{'#'*60}\n"
    )
    with open(output_path, "a", encoding="utf-8") as f:
        f.write(header)

    for ticker in watchlist:
        try:
            result = analyze(ticker)
            alpha = result["score"]["alpha_potential"]
            direction = result["score"]["consensus_direction"]
            strength = result["score"]["consensus_strength"]

            status = f"{ticker}: {direction.upper()} str={strength:.2f} alpha={alpha}"
            logger.info(status)

            with open(output_path, "a", encoding="utf-8") as f:
                if alpha in _ALPHA_THRESHOLDS:
                    idea = _format_idea(result, scan_time)
                    f.write(idea + "\n")
                    ideas_found += 1
                    logger.info(">>> IDEA FOUND: %s (%s)", ticker, alpha)
                else:
                    f.write(f"  {status} — no edge\n")
        except Exception as exc:
            logger.error("Analysis failed for %s: %s", ticker, exc)

    summary = f"\nScan complete: {ideas_found} ideas from {len(watchlist)} stocks\n"
    with open(output_path, "a", encoding="utf-8") as f:
        f.write(summary)
    logger.info(summary.strip())
    return ideas_found


async def run_monitor(
    duration_minutes: int = 120,
    scan_interval_minutes: int = 5,
    scrape_interval_seconds: int = 30,
    watchlist: list[str] | None = None,
) -> None:
    """Main monitor loop: scrape news + scan stocks on interval."""
    if watchlist is None:
        watchlist = WATCHLIST

    output_dir = Path.home() / ".tradingagents"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "trading_ideas.log"

    store = NewsStore()

    # Write session header
    start_time = datetime.now(tz=_CST)
    end_time = start_time + timedelta(minutes=duration_minutes)
    with open(output_path, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"CONTINUOUS MONITOR SESSION\n")
        f.write(f"Start: {start_time.strftime('%Y-%m-%d %H:%M CST')}\n")
        f.write(f"End:   {end_time.strftime('%Y-%m-%d %H:%M CST')}\n")
        f.write(f"Watchlist: {', '.join(watchlist)}\n")
        f.write(f"Scan interval: {scan_interval_minutes}min\n")
        f.write(f"{'='*60}\n")

    logger.info(
        "Monitor started. Duration=%dmin, scan every %dmin, watching %d stocks",
        duration_minutes, scan_interval_minutes, len(watchlist),
    )
    logger.info("Trading ideas log: %s", output_path)

    # Start background scraper
    scrape_task = asyncio.create_task(
        _scrape_loop(store, scrape_interval_seconds)
    )

    # Wait for initial data accumulation
    logger.info("Waiting 30s for initial news accumulation...")
    await asyncio.sleep(30)

    # Run scan loop
    try:
        elapsed = 0
        while elapsed < duration_minutes:
            _scan_all_stocks(watchlist, output_path)
            await asyncio.sleep(scan_interval_minutes * 60)
            elapsed += scan_interval_minutes
    finally:
        scrape_task.cancel()
        store.close()

    logger.info("Monitor session complete. Ideas saved to %s", output_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Continuous market monitor with trading idea generation"
    )
    parser.add_argument(
        "--duration", type=int, default=120,
        help="Total runtime in minutes (default: 120)",
    )
    parser.add_argument(
        "--scan-interval", type=int, default=5,
        help="Minutes between stock scans (default: 5)",
    )
    parser.add_argument(
        "--scrape-interval", type=int, default=30,
        help="Seconds between news scrapes (default: 30)",
    )
    parser.add_argument(
        "--tickers", type=str, default="",
        help="Comma-separated tickers to watch (default: built-in watchlist)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    watchlist = args.tickers.split(",") if args.tickers else None

    asyncio.run(run_monitor(
        duration_minutes=args.duration,
        scan_interval_minutes=args.scan_interval,
        scrape_interval_seconds=args.scrape_interval,
        watchlist=watchlist,
    ))


if __name__ == "__main__":
    main()
