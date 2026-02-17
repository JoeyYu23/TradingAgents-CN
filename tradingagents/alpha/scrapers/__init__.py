"""Scrapers for real-time news ingestion from Jin10 and BubbleSeek."""

from .store import NewsItem, NewsStore
from .jin10_scraper import Jin10Scraper
from .bubbleseek_scraper import BubbleSeekScraper
from .daemon import ScraperDaemon

__all__ = [
    "NewsItem",
    "NewsStore",
    "Jin10Scraper",
    "BubbleSeekScraper",
    "ScraperDaemon",
]
