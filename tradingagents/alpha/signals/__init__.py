"""Signal extraction modules for the Alpha Contradiction Engine."""

from .signal_types import Signal
from .technical_signal import extract as extract_technical
from .valuation_signal import extract as extract_valuation
from .insider_signal import extract as extract_insider
from .earnings_signal import extract as extract_earnings
from .options_signal import extract as extract_options
from .momentum_signal import extract as extract_momentum
from .macro_signal import extract as extract_macro
from .news_signal import extract as extract_news

__all__ = [
    "Signal",
    "extract_technical",
    "extract_valuation",
    "extract_insider",
    "extract_earnings",
    "extract_options",
    "extract_momentum",
    "extract_macro",
    "extract_news",
]
