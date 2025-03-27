import logging
from typing import List, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)

class TrendDirection(Enum):
    UP = "uptrend"
    DOWN = "downtrend"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"


class SimpleTrendAnalyzer:
    """
    A trend analyzer that determines market trend direction
    based solely on the pattern of swing highs and lows.
    """
    
    def __init__(self, lookback: int = 2):
        """
        Initialize the trend analyzer
        
        Args:
            lookback: Number of swing points to consider for trend determination
        """
        self.lookback = lookback
    
    def analyze_trend(self, swing_highs: List[Dict[str, Any]], swing_lows: List[Dict[str, Any]]) -> TrendDirection:
        """
        Analyze trend direction based on swing highs and lows
        
        Args:
            swing_highs: List of swing high points, each with at least 'price' and 'index' fields
            swing_lows: List of swing low points, each with at least 'price' and 'index' fields
            
        Returns:
            TrendDirection enum indicating the detected trend
        """
        # Check if we have enough swing points to determine a trend
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return TrendDirection.UNKNOWN
        
        # Sort swing points by index (chronological order)
        sorted_highs = sorted(swing_highs, key=lambda x: x['index'])
        sorted_lows = sorted(swing_lows, key=lambda x: x['index'])
        
        # Get the most recent swing points for analysis
        recent_highs = sorted_highs[-min(self.lookback, len(sorted_highs)):]
        recent_lows = sorted_lows[-min(self.lookback, len(sorted_lows)):]
        
        # Check for higher highs and higher lows (uptrend)
        higher_highs = all(recent_highs[i]['price'] > recent_highs[i-1]['price'] 
                       for i in range(1, len(recent_highs)))
        higher_lows = all(recent_lows[i]['price'] > recent_lows[i-1]['price'] 
                       for i in range(1, len(recent_lows)))
        
        # Check for lower highs and lower lows (downtrend)
        lower_highs = all(recent_highs[i]['price'] < recent_highs[i-1]['price'] 
                       for i in range(1, len(recent_highs)))
        lower_lows = all(recent_lows[i]['price'] < recent_lows[i-1]['price'] 
                      for i in range(1, len(recent_lows)))
        
        # Determine trend direction
        if higher_highs and higher_lows:
            return TrendDirection.UP
        elif lower_highs and lower_lows:
            return TrendDirection.DOWN
        else:
            return TrendDirection.NEUTRAL
    
    def update_market_context(self, market_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update market context with trend information
        
        Args:
            market_context: The current market context containing at least:
                           - swing_high_history: List of swing high points
                           - swing_low_history: List of swing low points
            
        Returns:
            Updated market context with trend information
        """
        # Extract swing histories from context
        swing_high_history = market_context.get('swing_high_history', [])
        swing_low_history = market_context.get('swing_low_history', [])
        
        # Analyze trend
        trend = self.analyze_trend(swing_high_history, swing_low_history)
        
        # Update market context with trend information
        prev_trend = market_context.get('trend', TrendDirection.UNKNOWN.value)
        market_context['trend'] = trend.value
        
        # Log if trend changed
        if prev_trend != trend.value:
            logger.info(f"Trend changed from {prev_trend} to {trend.value}")
        
        return market_context


# Example usage:
def example_trend_analysis(market_context):
    # Initialize analyzer
    analyzer = SimpleTrendAnalyzer(lookback=2)
    
    # Update market context with trend analysis
    updated_context = analyzer.update_market_context(market_context)
    
    return updated_context