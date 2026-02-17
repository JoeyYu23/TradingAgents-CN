"""Markdown report generator for the Alpha Contradiction Engine.

Produces a structured, data-driven report from signals and contradictions.
No LLM calls -- all output is deterministic template rendering.
"""

from __future__ import annotations

from datetime import date

from .contradiction_engine import Contradiction
from .data_collectors.yfinance_collector import StockData
from .signals.signal_types import Signal


class ReportGenerator:
    """Generate a human-readable markdown report from analysis results."""

    def generate(
        self,
        ticker: str,
        stock_data: StockData,
        signals: list[Signal],
        contradictions: list[Contradiction],
        score: dict,
    ) -> str:
        sections = [
            self._header(ticker, stock_data),
            self._signal_dashboard(signals),
            self._contradictions_section(contradictions),
            self._verdict(score, stock_data),
        ]
        return "\n\n".join(sections)

    def _header(self, ticker: str, data: StockData) -> str:
        return (
            f"# {ticker.upper()} Contradiction Analysis Report\n"
            f"**Date**: {date.today().isoformat()} | "
            f"**Price**: ${data.current_price:.2f}"
        )

    def _signal_dashboard(self, signals: list[Signal]) -> str:
        lines = [
            "## Signal Dashboard",
            "| Signal | Direction | Strength | Confidence | Key Data |",
            "|--------|-----------|----------|------------|----------|",
        ]
        for s in signals:
            key_data = self._summarize_data_points(s)
            lines.append(
                f"| {s.name.replace('_', ' ').title()} "
                f"| {s.direction.upper()} "
                f"| {s.strength:.1f} "
                f"| {s.confidence:.1f} "
                f"| {key_data} |"
            )
        return "\n".join(lines)

    def _contradictions_section(self, contradictions: list[Contradiction]) -> str:
        if not contradictions:
            return "## Contradictions Detected (0)\nNo significant contradictions found. Signals are largely aligned."

        lines = [f"## Contradictions Detected ({len(contradictions)})"]
        for i, c in enumerate(contradictions, 1):
            severity_marker = {"high": "!!!", "medium": "!!", "low": "!"}.get(
                c.severity, ""
            )
            lines.append(
                f"### {i}. {c.signal_a.name.upper()} vs "
                f"{c.signal_b.name.upper()} ({c.severity.title()} Severity) {severity_marker}"
            )
            lines.append(f"- **{c.signal_a.name.title()}**: {c.signal_a.reasoning}")
            lines.append(f"- **{c.signal_b.name.title()}**: {c.signal_b.reasoning}")
            lines.append(f"- **Historical**: {c.historical_resolution}")
            lines.append(f"- **Category**: {c.category}")
            lines.append("")
        return "\n".join(lines)

    def _verdict(self, score: dict, data: StockData) -> str:
        lines = ["## Verdict"]
        direction = score.get("consensus_direction", "unknown")
        strength = score.get("consensus_strength", 0)
        bull = score.get("bullish_count", 0)
        bear = score.get("bearish_count", 0)
        neutral = score.get("neutral_count", 0)
        c_count = score.get("contradiction_count", 0)
        alpha = score.get("alpha_potential", "none")

        lines.append(
            f"- **Consensus**: {direction.upper()} "
            f"(strength {strength:.2f}) — "
            f"{bull} bullish, {bear} bearish, {neutral} neutral"
        )
        lines.append(
            f"- **Contradictions**: {c_count} "
            f"(severity: {score.get('contradiction_severity', 'none')})"
        )
        lines.append(f"- **Alpha potential**: {alpha.upper()}")

        key_contras = score.get("key_contradictions", [])
        if key_contras:
            lines.append("- **Key risks**:")
            for kc in key_contras:
                lines.append(f"  - {kc}")

        if data.days_to_earnings is not None and data.days_to_earnings <= 14:
            lines.append(
                f"- **Event warning**: Earnings in {data.days_to_earnings} days "
                f"— may resolve or amplify contradictions"
            )

        return "\n".join(lines)

    def _summarize_data_points(self, signal: Signal) -> str:
        """Pick the most informative data points for the dashboard."""
        dp = signal.data_points
        parts: list[str] = []

        _KEY_FIELDS = [
            "rsi_14", "ma_cross", "pct_vs_50ma",
            "pe_used", "analyst_upside_pct",
            "insider_buys", "insider_sells",
            "put_call_ratio", "iv_regime",
            "return_5d_pct", "volume_confirmation",
            "vix", "vix_regime", "sector_vs_spy",
            "earnings_beats", "days_to_earnings", "event_risk",
            "ticker_sentiment", "macro_sentiment", "news_volume",
        ]

        for key in _KEY_FIELDS:
            if key in dp:
                val = dp[key]
                if isinstance(val, float):
                    parts.append(f"{key}={val:.1f}")
                else:
                    parts.append(f"{key}={val}")
            if len(parts) >= 3:
                break

        return ", ".join(parts) if parts else "-"
