import logging
from typing import List, Dict, Any, Optional
from .base import BaseAnalyzer

logger = logging.getLogger(__name__)

class SwingDetector(BaseAnalyzer):
    """
    A swing detector that identifies swing highs and lows in price data
    and updates market context when new swings are detected.
    """
    
    def __init__(self, lookback: int = 5, min_strength: float = 0.5):
        """
        Initialize the swing detector with parameters
        
        Args:
            lookback: Number of candles to look back/forward for swing confirmation
            min_strength: Minimum percentage change required for a valid swing (0.5 = 0.5%)
        """
        self.lookback = lookback
        self.min_strength_pct = min_strength / 100.0  # Convert to decimal
    
    def analyze(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Detect the most recent swing high and low points in the candle data
        
        Args:
            candles: List of candle dictionaries with OHLCV data
            
        Returns:
            Dictionary with the latest swing high and low
        """
        if len(candles) < self.lookback * 2 + 1:
            logger.warning(f"Not enough candles for swing detection. Need at least {self.lookback * 2 + 1}")
            return {"swing_high": None, "swing_low": None}
        
        # Extract price data
        highs = [c.get('high', c.get('close', 0)) for c in candles]
        lows = [c.get('low', c.get('close', 0)) for c in candles]
        timestamps = [c.get('timestamp', i) for i, c in enumerate(candles)]
        
        latest_swing_high = None
        latest_swing_low = None
        
        # Start from the lookback position and work towards the most recent candles
        # We stop at lookback from the end because we need future candles to confirm swings
        for i in range(self.lookback, len(candles) - self.lookback):
            # Check for swing high
            is_swing_high = all(highs[i] >= highs[i-j] for j in range(1, self.lookback+1)) and \
                            all(highs[i] >= highs[i+j] for j in range(1, self.lookback+1))
                           
            if is_swing_high:
                # Calculate swing strength (how significant is this swing)
                left_min = min(lows[max(0, i-self.lookback):i])
                swing_strength = (highs[i] - left_min) / left_min
                
                # Only consider if strength is significant
                if swing_strength >= self.min_strength_pct:
                    latest_swing_high = {
                        'price': highs[i],
                        'index': i,
                        'timestamp': timestamps[i],
                        'strength': swing_strength
                    }
            
            # Check for swing low
            is_swing_low = all(lows[i] <= lows[i-j] for j in range(1, self.lookback+1)) and \
                           all(lows[i] <= lows[i+j] for j in range(1, self.lookback+1))
                           
            if is_swing_low:
                # Calculate swing strength
                left_max = max(highs[max(0, i-self.lookback):i])
                swing_strength = (left_max - lows[i]) / left_max
                
                # Only consider if strength is significant
                if swing_strength >= self.min_strength_pct:
                    latest_swing_low = {
                        'price': lows[i],
                        'index': i,
                        'timestamp': timestamps[i],
                        'strength': swing_strength
                    }
        
        return {
            "swing_high": latest_swing_high,
            "swing_low": latest_swing_low
        }
    
    def update_market_context(self, context, candles: List[Dict[str, Any]]):
        """
        Update market context with newly detected swing points
        
        Args:
            context: MarketContext object to update
            candles: List of candle dictionaries with OHLCV data
            
        Returns:
            Updated MarketContext
        """
        # Detect latest swings
        swings = self.analyze(candles)
        
        # Update swing high if detected
        if swings['swing_high'] is not None:
            context.set_swing_high(swings['swing_high'])
            logger.info(f"New swing high detected at price {swings['swing_high']['price']}")
        
        # Update swing low if detected
        if swings['swing_low'] is not None:
            context.set_swing_low(swings['swing_low'])
            logger.info(f"New swing low detected at price {swings['swing_low']['price']}")
        
        return context
