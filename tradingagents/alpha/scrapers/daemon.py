"""Background daemon that periodically scrapes news sources."""

from __future__ import annotations

import argparse
import asyncio
import logging

from .bubbleseek_scraper import BubbleSeekScraper
from .jin10_scraper import Jin10Scraper
from .store import NewsStore

logger = logging.getLogger(__name__)


class ScraperDaemon:
    """Periodically fetches news from Jin10 and BubbleSeek into a local store."""

    def __init__(
        self,
        store: NewsStore,
        interval_seconds: int = 60,
    ) -> None:
        self.store = store
        self.interval = interval_seconds
        self.jin10 = Jin10Scraper()
        self.bubbleseek = BubbleSeekScraper()
        self._running = False

    async def run(self) -> None:
        """Start the scraping loop until stopped."""
        self._running = True
        logger.info("Scraper daemon started, interval=%ds", self.interval)
        while self._running:
            await self._scrape_jin10()
            await self._scrape_bubbleseek()
            self.store.cleanup(days_old=7)
            logger.info("Store contains %d items", self.store.count())
            await asyncio.sleep(self.interval)

    async def _scrape_jin10(self) -> None:
        """Fetch and store Jin10 flash news."""
        try:
            items = await self.jin10.fetch_latest_with_fallback()
            for item in items:
                self.store.save(item)
            logger.info("Jin10: saved %d items", len(items))
        except Exception as exc:
            logger.error("Jin10 scrape failed: %s", exc)

    async def _scrape_bubbleseek(self) -> None:
        """Fetch and store BubbleSeek news."""
        try:
            items = await self.bubbleseek.fetch_latest()
            for item in items:
                self.store.save(item)
            logger.info("BubbleSeek: saved %d items", len(items))
        except Exception as exc:
            logger.error("BubbleSeek scrape failed: %s", exc)

    def stop(self) -> None:
        """Signal the daemon loop to exit."""
        self._running = False
        logger.info("Scraper daemon stopping")


def _build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser for the scraper daemon."""
    parser = argparse.ArgumentParser(
        description="Run the news scraper daemon",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Scrape interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--jin10-only",
        action="store_true",
        help="Only scrape Jin10",
    )
    parser.add_argument(
        "--bubbleseek-only",
        action="store_true",
        help="Only scrape BubbleSeek",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="",
        help="Override default database path",
    )
    return parser


async def _run_with_args(args: argparse.Namespace) -> None:
    """Create store and daemon, then run according to CLI flags."""
    store = NewsStore(db_path=args.db_path)
    daemon = ScraperDaemon(store=store, interval_seconds=args.interval)

    if args.jin10_only:
        daemon._running = True
        logger.info("Running Jin10-only mode, interval=%ds", args.interval)
        while daemon._running:
            await daemon._scrape_jin10()
            store.cleanup(days_old=7)
            await asyncio.sleep(args.interval)
    elif args.bubbleseek_only:
        daemon._running = True
        logger.info("Running BubbleSeek-only mode, interval=%ds", args.interval)
        while daemon._running:
            await daemon._scrape_bubbleseek()
            store.cleanup(days_old=7)
            await asyncio.sleep(args.interval)
    else:
        await daemon.run()


def main() -> None:
    """CLI entry point for the scraper daemon."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run_with_args(args))


if __name__ == "__main__":
    main()
