import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from .base import BaseAnalyzer

logger = logging.getLogger(__name__)

@dataclass
class Range:
    """Represents a price range in the market"""
    high: float
    low: float
    equilibrium: float
    start_index: int
    end_index: int
    strength: float  # 0.0 to 1.0
    touches: int     # Number of times price touched range boundaries
    timestamp: Optional[str] = None  # When the range was detected

class RangeDetector(BaseAnalyzer):
    """
    Detects and validates price ranges using the six-candle rule
    and other technical criteria.
    """
    
    def __init__(self, min_touches: int = 3, min_range_size: float = 0.5, max_lookback: int = 100):
        """
        Initialize range detector
        
        Args:
            min_touches: Minimum number of touches required for valid range
            min_range_size: Minimum range size as percentage
            max_lookback: Maximum number of candles to look back for range detection
        """
        self.min_touches = min_touches
        self.min_range_size = min_range_size / 100.0  # Convert to decimal
        self.max_lookback = max_lookback
    
    def analyze(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Find potential ranges in price data
        
        Args:
            candles: List of OHLCV candles
            
        Returns:
            Dictionary with range information
        """
        ranges = self.detect_ranges(candles)
        
        if not ranges:
            return {
                "has_range": False,
                "range": None
            }
        
        # Use most recent valid range
        latest_range = ranges[-1]
        
        return {
            "has_range": True,
            "range": latest_range
        }
    
    def detect_ranges(self, candles: List[Dict[str, Any]]) -> List[Range]:
        """
        Find potential ranges in price data
        
        Args:
            candles: List of OHLCV candles
            
        Returns:
            List of detected ranges
        """
        ranges = []
        if len(candles) < 6:  # Need minimum 6 candles
            return ranges
            
        # Implement six-candle rule and range detection logic
        for i in range(5, len(candles)):
            potential_range = self._check_six_candle_rule(candles[i-5:i+1])
            if potential_range:
                # Validate range with subsequent price action
                if self._validate_range(candles[i+1:], potential_range):
                    ranges.append(potential_range)
        
        return ranges
    
    def _check_six_candle_rule(self, six_candles: List[Dict[str, Any]]) -> Optional[Range]:
        """Apply six-candle rule to detect potential range"""
        if len(six_candles) != 6:
            return None
            
        # Get high and low of first 5 candles
        first_five_high = max(c['high'] for c in six_candles[:5])
        first_five_low = min(c['low'] for c in six_candles[:5])
        
        # Check if 6th candle stays within range
        sixth_candle = six_candles[5]
        if (sixth_candle['high'] <= first_five_high and 
            sixth_candle['low'] >= first_five_low):
            
            # Calculate range size as percentage
            range_size = (first_five_high - first_five_low) / first_five_low
            
            if range_size >= self.min_range_size:
                # Get timestamp from the 6th candle if available
                timestamp = sixth_candle.get('timestamp', datetime.now().isoformat())
                
                return Range(
                    high=first_five_high,
                    low=first_five_low,
                    equilibrium=(first_five_high + first_five_low) / 2,
                    start_index=0,
                    end_index=5,
                    strength=0.5,  # Initial strength
                    touches=1,     # Initial touch count
                    timestamp=timestamp
                )
        
        return None
    
    def _validate_range(self, subsequent_candles: List[Dict[str, Any]], 
                       range_: Range) -> bool:
        """
        Validate range with subsequent price action
        
        Args:
            subsequent_candles: Candles after potential range
            range_: Range to validate
            
        Returns:
            True if range is valid
        """
        touches = range_.touches
        
        for candle in subsequent_candles:
            # Count touches of range boundaries
            if abs(candle['high'] - range_.high) / range_.high < 0.001:
                touches += 1
            if abs(candle['low'] - range_.low) / range_.low < 0.001:
                touches += 1
                
            # Break range if price moves significantly beyond boundaries
            if candle['high'] > range_.high * 1.02 or candle['low'] < range_.low * 0.98:
                return False
        
        return touches >= self.min_touches
    
    def is_price_in_range(self, price: float, range_high: float, range_low: float, tolerance: float = 0.005) -> bool:
        """
        Check if a price is within a specified range with tolerance
        
        Args:
            price: Price to check
            range_high: Upper bound of the range
            range_low: Lower bound of the range
            tolerance: Percentage tolerance for range boundaries
            
        Returns:
            True if price is within range (with tolerance), False otherwise
        """
        if range_high is None or range_low is None:
            return False
            
        upper_bound = range_high * (1 + tolerance)
        lower_bound = range_low * (1 - tolerance)
        
        return lower_bound <= price <= upper_bound
    
    def update_market_context(self, context, candles: List[Dict[str, Any]]):
        """
        Update market context with range information
        
        Args:
            context: MarketContext object to update
            candles: List of candles
            
        Returns:
            Updated MarketContext
        """
        # Detect ranges
        analysis_result = self.analyze(candles)
        
        if analysis_result["has_range"]:
            # Get the detected range
            range_ = analysis_result["range"]
            
            # Update context with range information
            context.set_range(
                range_.high,
                range_.low,
                range_.equilibrium,
                range_.strength,
                range_.timestamp
            )
        else:
            # Clear range information
            context.clear_range()
        
        return context
