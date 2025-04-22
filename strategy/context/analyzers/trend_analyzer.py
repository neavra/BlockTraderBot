import logging
from typing import List, Dict, Any
from .base import BaseAnalyzer
from strategy.domain.types.trend_direction_enum import TrendDirectionEnum

logger = logging.getLogger(__name__)

class TrendAnalyzer(BaseAnalyzer):
    """
    A trend analyzer that determines market trend direction
    based on the pattern of swing highs and lows.
    """
    
    def __init__(self, lookback: int = 2):
        """
        Initialize the trend analyzer
        
        Args:
            lookback: Number of swing points to consider for trend determination
        """
        self.lookback = lookback
    
    def analyze(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        This method is not directly used for trend analysis since it requires
        swing points from the market context rather than raw candles.
        
        Returns:
            Empty dictionary as trend analysis is done in update_market_context
        """
        # Trend analysis requires swing points which are in the market context
        # So this method doesn't do much on its own
        return {}
    
    def analyze_trend(self, swing_highs: List[Dict[str, Any]], swing_lows: List[Dict[str, Any]]) -> TrendDirectionEnum:
        """
        Analyze trend direction based on swing highs and lows
        
        Args:
            swing_highs: List of swing high points, each with at least 'price' and 'index' fields
            swing_lows: List of swing low points, each with at least 'price' and 'index' fields
            
        Returns:
            TrendDirectionEnum enum indicating the detected trend
        """
        # Check if we have enough swing points to determine a trend
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return TrendDirectionEnum.UNKNOWN
        
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
            return TrendDirectionEnum.UP
        elif lower_highs and lower_lows:
            return TrendDirectionEnum.DOWN
        else:
            return TrendDirectionEnum.NEUTRAL
    
    def update_market_context(self, context, candles: List[Dict[str, Any]]):
        """
        Update market context with trend information
        
        Args:
            context: MarketContext object to update
            candles: List of candle data (not used directly)
            
        Returns:
            Updated MarketContext
        """
        # Extract swing histories from context
        swing_high_history = context.swing_high_history
        swing_low_history = context.swing_low_history
        
        # Analyze trend
        trend = self.analyze_trend(swing_high_history, swing_low_history)
        
        # Update market context with trend information
        prev_trend = context.trend
        context.set_trend(trend.value)
        
        # Log if trend changed
        if prev_trend != trend.value:
            logger.info(f"Trend changed from {prev_trend} to {trend.value}")
        
        return context
