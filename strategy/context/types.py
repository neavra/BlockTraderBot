from enum import Enum
from typing import Dict, List

class TimeframeCategory(Enum):
    HTF = "high_timeframe"
    MTF = "medium_timeframe"
    LTF = "low_timeframe"

class TrendDirection(Enum):
    UP = "uptrend"
    DOWN = "downtrend"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"

# Mapping timeframes to categories
TIMEFRAME_CATEGORIES: Dict[str, TimeframeCategory] = {
    # High timeframes
    "1d": TimeframeCategory.HTF,
    "4h": TimeframeCategory.HTF,
    "1h": TimeframeCategory.HTF,
    # Medium timeframes
    "30m": TimeframeCategory.MTF,
    "15m": TimeframeCategory.MTF,
    # Low timeframes
    "5m": TimeframeCategory.LTF,
    "1m": TimeframeCategory.LTF
}

def get_timeframe_category(timeframe: str) -> TimeframeCategory:
    """Get category for a specific timeframe"""
    return TIMEFRAME_CATEGORIES.get(timeframe, TimeframeCategory.MTF)