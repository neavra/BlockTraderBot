from enum import Enum

class TimeframeCategoryEnum(Enum):
    """Categorization of timeframes based on duration"""
    HTF = "high_timeframe"  # High timeframe (1h and above)
    MTF = "medium_timeframe"  # Medium timeframe (15m to 30m)
    LTF = "low_timeframe"  # Low timeframe (below 15m)


class TimeframeEnum(Enum):
    """Standard trading timeframes"""
    M1 = "1m"    # 1 minute
    M5 = "5m"    # 5 minutes
    M15 = "15m"  # 15 minutes
    M30 = "30m"  # 30 minutes
    H1 = "1h"    # 1 hour
    H4 = "4h"    # 4 hours
    D1 = "1d"    # 1 day
    W1 = "1w"    # 1 week
    MN = "1M"    # 1 month


# Define the categories mapping outside the enum
_TIMEFRAME_CATEGORIES = {
    "1d": TimeframeCategoryEnum.HTF,
    "4h": TimeframeCategoryEnum.HTF,
    "1h": TimeframeCategoryEnum.HTF,
    "30m": TimeframeCategoryEnum.MTF,
    "15m": TimeframeCategoryEnum.MTF,
    "5m": TimeframeCategoryEnum.LTF,
    "1m": TimeframeCategoryEnum.LTF
}


def get_timeframe_category(timeframe: str) -> TimeframeCategoryEnum:
    """
    Get category for a specific timeframe
    
    Args:
        timeframe: Timeframe string (e.g., "1m", "5m", "1h")
        
    Returns:
        TimeframeCategoryEnum enum value (defaults to MTF if not found)
    """
    return _TIMEFRAME_CATEGORIES.get(timeframe, TimeframeCategoryEnum.MTF)


# Add instance method to TimeframeEnum
def _get_category(self) -> TimeframeCategoryEnum:
    """
    Get the category for this timeframe instance
    
    Returns:
        TimeframeCategoryEnum enum value
    """
    return get_timeframe_category(self.value)


# Add the method to the class
TimeframeEnum.category = _get_category