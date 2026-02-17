"""Core signal dataclass used by all signal extractors."""

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Signal:
    """A directional signal extracted from market data."""

    name: str
    direction: Literal["bullish", "bearish", "neutral"]
    strength: float  # 0.0 to 1.0
    confidence: float  # 0.0 to 1.0 (data quality / completeness)
    data_points: dict = field(default_factory=dict)
    reasoning: str = ""
