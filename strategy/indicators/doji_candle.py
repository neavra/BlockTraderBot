from typing import Dict, Any, List
from indicators.base import Indicator
import logging

logger = logging.getLogger(__name__)

class DojiCandleIndicator(Indicator):
    """
    Indicator that detects Doji candle patterns.
    
    A Doji candle is characterized by having a very small body (open and close prices are nearly equal)
    relative to its range (high and low). It indicates market indecision and potential reversal points.
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize Doji candle detector with parameters
        
        Args:
            params: Dictionary containing parameters:
                - max_body_to_range_ratio: Maximum ratio of body to total range to qualify as doji
                - min_range_to_price_ratio: Minimum candle range relative to price for significance
                - lookback_period: Number of candles to analyze
        """
        default_params = {
            'max_body_to_range_ratio': 0.1,     # Maximum body/range ratio to qualify as doji
            'min_range_to_price_ratio': 0.005,  # Minimum range/price ratio (filters out tiny dojis)
            'lookback_period': 20               # Number of candles to look back
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect Doji candle patterns in the provided data
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles
                - current_price: Current market price (optional)
                
        Returns:
            Dictionary with detected doji patterns:
                - dojis: List of detected doji candles with details
                - has_doji: Boolean indicating if a doji was found in recent candles
                - latest_doji: Most recent doji candle details or None
        """
        candles = data.get('candles', [])
        
        # Need enough candles to analyze
        if len(candles) < 3:
            logger.warning("Not enough candles to detect doji patterns (minimum 3 required)")
            return {
                'dojis': [],
                'has_doji': False,
                'latest_doji': None
            }
        
        lookback_period = min(self.params['lookback_period'], len(candles))
        dojis = []
        
        # Process each candle in the lookback period (from most recent)
        for i in range(1, lookback_period + 1):
            candle_idx = len(candles) - i
            if candle_idx < 0:
                break
                
            candle = candles[candle_idx]
            
            # Calculate key metrics for doji identification
            body_size = abs(candle['close'] - candle['open'])
            total_range = candle['high'] - candle['low']
            
            # Avoid division by zero
            if total_range == 0:
                continue
                
            body_to_range_ratio = body_size / total_range
            
            # Calculate total wick size
            total_wick_size = total_range - body_size
            
            # Check price-relative size (to filter out insignificant dojis)
            avg_price = (candle['high'] + candle['low']) / 2
            range_to_price_ratio = total_range / avg_price
            
            # Basic doji qualification: small body relative to range and significant range
            if (body_to_range_ratio <= self.params['max_body_to_range_ratio'] and 
                range_to_price_ratio >= self.params['min_range_to_price_ratio']):
                
                # Create doji object with details
                doji = {
                    'index': candle_idx,
                    'body_to_range_ratio': body_to_range_ratio,
                    'total_wick_size': total_wick_size,
                    'candle': candle.copy(),  # Include the original candle data
                    'strength': 1.0 - body_to_range_ratio  # Higher strength for smaller bodies
                }
                
                # Add timestamp if available
                if 'timestamp' in candle:
                    doji['timestamp'] = candle['timestamp']
                
                dojis.append(doji)
        
        # Sort dojis by index (most recent first)
        dojis.sort(key=lambda x: x['index'], reverse=True)
        
        # Prepare result
        result = {
            'dojis': dojis,
            'has_doji': len(dojis) > 0,
            'latest_doji': dojis[0] if dojis else None
        }
        
        return result
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns data requirements for doji detection
        
        Returns:
            Dictionary with requirements
        """
        return {
            'candles': True,
            'lookback_period': self.params['lookback_period'],
            'timeframes': ['1m', '5m', '15m', '30m', '1h', '4h', '1d'],  # Supported timeframes
            'indicators': []  # No dependency on other indicators
        }