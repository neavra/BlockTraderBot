from enum import Enum

class TrendDirectionEnum(Enum):
    """Direction of market trend"""
    UP = "uptrend"
    DOWN = "downtrend"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"