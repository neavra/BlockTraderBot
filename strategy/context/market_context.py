from typing import Dict, Any, Optional
from datetime import datetime
import logging
from .types import TrendDirection, get_timeframe_category

logger = logging.getLogger(__name__)

class MarketContext:
    """Rich domain model representing market state for specific symbol/timeframe"""

    def __init__(self, symbol: str, timeframe: str, exchange: str = 'default'):
        self.symbol = symbol
        self.timeframe = timeframe
        self.exchange = exchange
        self.timeframe_category = get_timeframe_category(timeframe)

        # Basic info
        self.timestamp = None
        self.current_price = None

        # Swing points
        self.swing_high = None
        self.swing_low = None
        self.swing_high_history = []
        self.swing_low_history = []

        # Trend information
        self.trend = TrendDirection.UNKNOWN.value

        # Range information
        self.range_high = None
        self.range_low = None
        self.range_equilibrium = None
        self.is_in_range = False
        self.range_size = None
        self.range_strength = None
        self.range_detected_at = None

        # Metadata
        self.last_updated = datetime.now().timestamp()

    # Basic info methods
    def set_current_price(self, price: float):
        """Set current price"""
        self.current_price = price
        return self

    def get_current_price(self) -> Optional[float]:
        """Get current price"""
        return self.current_price

    # Swing point methods
    def set_swing_high(self, swing_high: Dict[str, Any]):
        """Set new swing high and update history"""
        if self.swing_high is None or swing_high['index'] != self.swing_high.get('index'):
            self.swing_high = swing_high
            self.swing_high_history.insert(0, swing_high)  # Add to front of list
            # Limit history size if needed
            if len(self.swing_high_history) > 10:  # Example limit
                self.swing_high_history = self.swing_high_history[:10]
        return self

    def set_swing_low(self, swing_low: Dict[str, Any]):
        """Set new swing low and update history"""
        if self.swing_low is None or swing_low['index'] != self.swing_low.get('index'):
            self.swing_low = swing_low
            self.swing_low_history.insert(0, swing_low)  # Add to front of list
            # Limit history size if needed
            if len(self.swing_low_history) > 10:  # Example limit
                self.swing_low_history = self.swing_low_history[:10]
        return self

    def get_latest_swing_high(self) -> Optional[Dict[str, Any]]:
        """Get most recent swing high"""
        return self.swing_high_history[0] if self.swing_high_history else None

    def get_latest_swing_low(self) -> Optional[Dict[str, Any]]:
        """Get most recent swing low"""
        return self.swing_low_history[0] if self.swing_low_history else None

    # Trend methods
    def set_trend(self, trend: str):
        """Set current trend direction"""
        self.trend = trend
        return self

    def get_current_trend(self) -> str:
        """Get current trend direction"""
        return self.trend

    # Range methods
    def set_range(self, high: float, low: float, equilibrium: float, strength: float = 0.5, timestamp: str = None):
        """Set range information"""
        self.range_high = high
        self.range_low = low
        self.range_equilibrium = equilibrium
        self.range_strength = strength
        self.range_detected_at = timestamp or datetime.now().isoformat()

        # Calculate range size as percentage of lower bound
        self.range_size = (high - low) / low if low > 0 else 0

        # Set in-range flag
        self.is_in_range = True
        return self

    def clear_range(self):
        """Clear range information"""
        self.range_high = None
        self.range_low = None
        self.range_equilibrium = None
        self.is_in_range = False
        self.range_size = None
        self.range_strength = None
        self.range_detected_at = None
        return self

    def check_if_in_range(self, price: float, tolerance: float = 0.005) -> bool:
        """Check if price is within current range"""
        if not self.range_high or not self.range_low:
            return False

        return self.range_low * (1 - tolerance) <= price <= self.range_high * (1 + tolerance)

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for storage"""
        return {
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'exchange': self.exchange,
            'timestamp': self.timestamp,
            'current_price': self.current_price,
            'swing_high': self.swing_high,
            'swing_low': self.swing_low,
            'swing_high_history': self.swing_high_history,
            'swing_low_history': self.swing_low_history,
            'trend': self.trend,
            'range_high': self.range_high,
            'range_low': self.range_low,
            'range_equilibrium': self.range_equilibrium,
            'is_in_range': self.is_in_range,
            'range_size': self.range_size,
            'range_strength': self.range_strength,
            'range_detected_at': self.range_detected_at,
            'last_updated': self.last_updated
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MarketContext':
        """Create context from dictionary"""
        context = cls(data['symbol'], data['timeframe'])
        for key, value in data.items():
            setattr(context, key, value)
        return context
