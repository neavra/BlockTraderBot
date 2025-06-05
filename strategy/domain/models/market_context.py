from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, field, fields
import logging
from strategy.domain.types.time_frame_enum import TimeframeCategoryEnum, get_timeframe_category
from strategy.domain.types.trend_direction_enum import TrendDirectionEnum

logger = logging.getLogger(__name__)

@dataclass
class MarketContext:
    """Domain model representing market state for a specific symbol/timeframe"""
    symbol: str
    timeframe: str
    exchange: str = "default"
    
    # Basic info
    timestamp: Optional[str] = None  # Store as ISO string
    current_price: Optional[float] = None
    
    # Swing points
    swing_high: Optional[Dict[str, Any]] = None
    swing_low: Optional[Dict[str, Any]] = None
    
    # Trend information
    trend: str = field(default_factory=lambda: TrendDirectionEnum.UNKNOWN.value)
    
    # Range information
    range_high: Optional[float] = None
    range_low: Optional[float] = None
    range_equilibrium: Optional[float] = None
    is_in_range: bool = False
    range_size: Optional[float] = None
    range_strength: Optional[float] = None
    range_detected_at: Optional[str] = None  # Store as ISO string
    
    # Fibonacci levels
    fib_levels: Dict[str, List[Dict[str, Any]]] = field(default_factory=lambda: {"support": [], "resistance": []})
    
    # Metadata - Store as timestamp float
    last_updated: float = field(default_factory=lambda: datetime.now().timestamp())
    
    # Computed property (post-init)
    timeframe_category: TimeframeCategoryEnum = field(init=False)
    
    def __post_init__(self):
        """Initialize computed properties after data class initialization"""
        # Use the standalone function instead of the enum method
        self.timeframe_category = get_timeframe_category(self.timeframe)
        
        # Ensure timestamp is set as ISO string
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
    
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
        """Set new swing high with proper serialization"""
        self.swing_high = self._serialize_swing_point(swing_high) if swing_high else None
        return self

    def set_swing_low(self, swing_low: Dict[str, Any]):
        """Set new swing low with proper serialization"""
        self.swing_low = self._serialize_swing_point(swing_low) if swing_low else None
        return self

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
        # Always store as ISO string
        self.range_detected_at = timestamp if timestamp else datetime.now().isoformat()

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
    
    # Fibonacci level methods
    def set_fib_levels(self, fib_levels: Dict[str, List[Dict[str, Any]]]):
        """Set Fibonacci levels"""
        self.fib_levels = fib_levels
        return self
    
    def get_nearest_fib_level(self, price: float, level_type: str = 'all', max_distance_percent: float = 1.0) -> Optional[Dict[str, Any]]:
        """Find the nearest Fibonacci level to the current price"""
        levels = []
        
        if level_type in ['support', 'all']:
            levels.extend(self.fib_levels.get('support', []))
            
        if level_type in ['resistance', 'all']:
            levels.extend(self.fib_levels.get('resistance', []))
            
        if not levels:
            return None
            
        # Calculate distances
        for level in levels:
            level_price = level.get('price', 0)
            level['distance'] = abs(price - level_price)
            level['distance_percent'] = (level['distance'] / price) * 100
            
        # Filter by maximum distance
        valid_levels = [l for l in levels if l.get('distance_percent', 100) <= max_distance_percent]
        
        if not valid_levels:
            return None
            
        # Return the closest level
        return min(valid_levels, key=lambda x: x.get('distance', float('inf')))

    def _serialize_swing_point(self, swing_point: Dict[str, Any]) -> Dict[str, Any]:
        """
        Helper to serialize swing point data - convert ALL datetime fields to ISO strings
        """
        if not swing_point or not isinstance(swing_point, dict):
            return swing_point
        
        swing_copy = swing_point.copy()
        
        # List of known datetime fields in swing points
        datetime_fields = ['timestamp', 'expiry', 'created_at', 'updated_at']
        
        for field in datetime_fields:
            if field in swing_copy and isinstance(swing_copy[field], datetime):
                swing_copy[field] = swing_copy[field].isoformat()
        
        return swing_copy

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for storage - all datetime objects converted to strings"""
        return {
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'exchange': self.exchange,
            'timestamp': self.timestamp,  # Already a string
            'current_price': self.current_price,
            'swing_high': self.swing_high,  # Already serialized in setter
            'swing_low': self.swing_low,    # Already serialized in setter
            'trend': self.trend,
            'range_high': self.range_high,
            'range_low': self.range_low,
            'range_equilibrium': self.range_equilibrium,
            'is_in_range': self.is_in_range,
            'range_size': self.range_size,
            'range_strength': self.range_strength,
            'range_detected_at': self.range_detected_at,  # Already a string
            'fib_levels': self.fib_levels,
            'last_updated': self.last_updated,  # Keep as float timestamp
            'timeframe_category': self.timeframe_category.value if hasattr(self.timeframe_category, 'value') else str(self.timeframe_category)
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MarketContext':
        """
        Create a MarketContext object from a dictionary.
        
        Args:
            data: Dictionary containing MarketContext data
            
        Returns:
            MarketContext object
        """
        def parse_datetime_for_swing(value):
            """Helper function to parse datetime from various formats for swing points"""
            if value is None:
                return None
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    # Try parsing ISO format
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    try:
                        # Try parsing timestamp format
                        return datetime.fromtimestamp(float(value))
                    except (ValueError, TypeError):
                        return None
            if isinstance(value, (int, float)):
                try:
                    return datetime.fromtimestamp(value)
                except (ValueError, OSError):
                    return None
            return None
        
        def parse_swing_point(swing_data):
            """Helper function to parse swing point data"""
            if swing_data is None or not isinstance(swing_data, dict):
                return swing_data
            
            # Create a copy to avoid modifying original data
            parsed_swing = swing_data.copy()
            
            # Parse datetime fields in swing point if they exist
            datetime_fields = ['timestamp', 'expiry', 'created_at', 'updated_at']
            for field in datetime_fields:
                if field in parsed_swing:
                    parsed_swing[field] = parse_datetime_for_swing(parsed_swing[field])
            
            return parsed_swing
        
        # Create a new instance
        context = cls(
            symbol=data.get('symbol'),
            timeframe=data.get('timeframe'),
            exchange=data.get('exchange', 'default')
        )
        
        # Set all the attributes from the dictionary
        context.timestamp = data.get('timestamp')  # Keep as string
        context.last_updated = data.get('last_updated', datetime.now().timestamp())  # Keep as float
        context.current_price = data.get('current_price')
        
        # Parse swing points with datetime conversion
        context.swing_high = parse_swing_point(data.get('swing_high'))
        context.swing_low = parse_swing_point(data.get('swing_low'))
        
        context.trend = data.get('trend', TrendDirectionEnum.UNKNOWN.value)
        context.range_high = data.get('range_high')
        context.range_low = data.get('range_low')
        context.range_equilibrium = data.get('range_equilibrium')
        context.range_size = data.get('range_size')
        context.range_strength = data.get('range_strength')
        context.range_detected_at = data.get('range_detected_at')  # Keep as string
        context.is_in_range = data.get('is_in_range', False)
        context.fib_levels = data.get('fib_levels', {"support": [], "resistance": []})
        
        # Handle timeframe_category
        timeframe_category = data.get('timeframe_category')
        if timeframe_category:
            try:
                context.timeframe_category = TimeframeCategoryEnum(timeframe_category)
            except ValueError:
                # Fallback to computing it
                context.timeframe_category = get_timeframe_category(context.timeframe)
        
        return context
    
    def is_complete(self) -> bool:
        """Check if context has all essential components"""
        return all([
            self.symbol,
            self.timeframe,
            self.exchange,
            self.swing_high is not None,
            self.swing_low is not None,
            self.fib_levels is not None,
            self.current_price is not None
        ])