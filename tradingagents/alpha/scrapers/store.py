"""SQLite-based local storage for scraped news items."""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_DB_DIR = Path.home() / ".tradingagents"
_DEFAULT_DB_PATH = _DEFAULT_DB_DIR / "news.db"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS news_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT    NOT NULL,
    category    TEXT    NOT NULL,
    title       TEXT    NOT NULL,
    content     TEXT    NOT NULL,
    ticker      TEXT    NOT NULL DEFAULT '',
    published_at TEXT   NOT NULL,
    scraped_at  TEXT    NOT NULL,
    importance  TEXT    NOT NULL DEFAULT 'low',
    raw_data    TEXT    NOT NULL DEFAULT '{}',
    UNIQUE(source, title, published_at)
);
"""

_CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_ticker_published ON news_items (ticker, published_at);",
    "CREATE INDEX IF NOT EXISTS idx_source_published ON news_items (source, published_at);",
]


@dataclass
class NewsItem:
    """A single scraped news item."""

    source: str  # "jin10" | "bubbleseek"
    category: str  # "macro" | "stock_news" | "kol" | "options_anomaly"
    title: str
    content: str
    ticker: str  # "" for macro news, specific ticker for stock news
    published_at: datetime
    scraped_at: datetime
    importance: str  # "high" | "medium" | "low"
    raw_data: dict = field(default_factory=dict)


class NewsStore:
    """SQLite store for news items with deduplication.

    Uses WAL journal mode for safe concurrent access across processes
    (e.g. daemon writing while runner reads).
    """

    def __init__(self, db_path: str = "") -> None:
        resolved = Path(db_path).expanduser() if db_path else _DEFAULT_DB_PATH
        resolved.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = str(resolved)
        self._conn = sqlite3.connect(
            self._db_path, check_same_thread=False,
        )
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        """Close the underlying database connection."""
        self._conn.close()

    def __enter__(self) -> NewsStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _init_schema(self) -> None:
        """Create table and indexes if they don't exist."""
        cur = self._conn.cursor()
        cur.execute(_CREATE_TABLE_SQL)
        for idx_sql in _CREATE_INDEXES_SQL:
            cur.execute(idx_sql)
        self._conn.commit()

    def save(self, item: NewsItem) -> None:
        """Insert a news item, silently skipping duplicates."""
        cur = self._conn.cursor()
        cur.execute(
            """INSERT OR IGNORE INTO news_items
               (source, category, title, content, ticker,
                published_at, scraped_at, importance, raw_data)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                item.source,
                item.category,
                item.title,
                item.content,
                item.ticker,
                item.published_at.isoformat(),
                item.scraped_at.isoformat(),
                item.importance,
                json.dumps(item.raw_data, ensure_ascii=False),
            ),
        )
        self._conn.commit()

    def query(
        self,
        ticker: str = "",
        source: str = "",
        hours_back: int = 24,
        limit: int = 50,
        *,
        filter_ticker: bool = False,
    ) -> list[NewsItem]:
        """Filter news items by ticker, source, and recency.

        When filter_ticker is True, the ticker value is used as an exact
        match (including empty string for macro-only news).
        When False (default), an empty ticker skips the filter.
        """
        cutoff = (
            datetime.now(tz=timezone.utc) - timedelta(hours=hours_back)
        ).isoformat()

        clauses: list[str] = ["published_at >= ?"]
        params: list[str | int] = [cutoff]

        if ticker or filter_ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        if source:
            clauses.append("source = ?")
            params.append(source)

        where = " AND ".join(clauses)
        params.append(limit)

        cur = self._conn.cursor()
        cur.execute(
            f"SELECT * FROM news_items WHERE {where} "
            "ORDER BY published_at DESC LIMIT ?",
            params,
        )
        return [self._row_to_item(row) for row in cur.fetchall()]

    def query_macro(self, hours_back: int = 24) -> list[NewsItem]:
        """Shortcut to query macro news (items with empty ticker)."""
        return self.query(ticker="", hours_back=hours_back, filter_ticker=True)

    def count(self) -> int:
        """Return total number of stored news items."""
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM news_items")
        return cur.fetchone()[0]

    def cleanup(self, days_old: int = 7) -> int:
        """Delete rows older than N days. Returns count deleted."""
        cutoff = (
            datetime.now(tz=timezone.utc) - timedelta(days=days_old)
        ).isoformat()
        cur = self._conn.cursor()
        cur.execute(
            "DELETE FROM news_items WHERE published_at < ?", (cutoff,)
        )
        deleted = cur.rowcount
        self._conn.commit()
        logger.info("Cleaned up %d news items older than %d days", deleted, days_old)
        return deleted

    @staticmethod
    def _row_to_item(row: sqlite3.Row) -> NewsItem:
        """Convert a database row to a NewsItem."""
        return NewsItem(
            source=row["source"],
            category=row["category"],
            title=row["title"],
            content=row["content"],
            ticker=row["ticker"],
            published_at=datetime.fromisoformat(row["published_at"]),
            scraped_at=datetime.fromisoformat(row["scraped_at"]),
            importance=row["importance"],
            raw_data=json.loads(row["raw_data"]),
        )
