# strategy/context/trend_analysis.py
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class TrendDirection(Enum):
    UP = "uptrend"
    DOWN = "downtrend"
    SIDEWAYS = "sideways"
    UNKNOWN = "unknown"

class TrendAnalyzer:
    """
    Analyzes price data and swing points to determine market trends
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize trend analyzer with parameters
        
        Args:
            params: Dictionary with trend analysis parameters:
                - method: Trend detection method
                - lookback: Number of swing points to consider
                - threshold: Minimum relative change to confirm trend change
        """
        default_params = {
            'method': 'swing',       # 'swing', 'ema', 'linear_regression'
            'lookback': 4,           # Number of swing points to consider
            'threshold': 1.0,        # % threshold for trend change confirmation
            'sideways_threshold': 0.5,  # % threshold for sideways market
            'swing_confirmation': 2   # Number of swing points needed to confirm trend
        }
        
        if params:
            default_params.update(params)
        
        self.params = default_params
    
    def analyze_trend(self, candles: List[Dict[str, Any]], 
                    swings: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Analyze trend based on candle data and swing points
        
        Args:
            candles: List of candle dictionaries with OHLCV data
            swings: Dictionary with swing highs and lows
            
        Returns:
            Dictionary with trend analysis results
        """
        method = self.params['method'].lower()
        
        if method == 'swing':
            return self._analyze_swing_trend(candles, swings)
        elif method == 'ema':
            return self._analyze_ema_trend(candles)
        elif method == 'linear_regression':
            return self._analyze_linear_trend(candles)
        else:
            logger.warning(f"Unknown trend analysis method: {method}. Using swing method.")
            return self._analyze_swing_trend(candles, swings)
    
    def _analyze_swing_trend(self, candles: List[Dict[str, Any]], 
                          swings: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Analyze trend based on swing highs and lows
        
        This uses the sequence of swing highs and lows to determine trend direction:
        - Higher highs and higher lows = Uptrend
        - Lower highs and lower lows = Downtrend
        - Mixed or sideways swings = Sideways
        """
        # Get swing points
        highs = swings.get('highs', [])
        lows = swings.get('lows', [])
        
        # Not enough swing points
        lookback = self.params['lookback']
        if len(highs) < 2 or len(lows) < 2:
            return {
                'trend': TrendDirection.UNKNOWN.value,
                'strength': 0.0,
                'swings': {'highs': highs, 'lows': lows},
                'structure_breaks': []
            }
        
        # Sort swing points by index (time)
        sorted_highs = sorted(highs, key=lambda x: x['index'])
        sorted_lows = sorted(lows, key=lambda x: x['index'])
        
        # Get the most recent swing points (up to lookback)
        recent_highs = sorted_highs[-min(lookback, len(sorted_highs)):]
        recent_lows = sorted_lows[-min(lookback, len(sorted_lows)):]
        
        # Check higher highs and higher lows (uptrend)
        higher_highs = all(recent_highs[i]['price'] >= recent_highs[i-1]['price'] 
                        for i in range(1, len(recent_highs)))
        higher_lows = all(recent_lows[i]['price'] >= recent_lows[i-1]['price'] 
                        for i in range(1, len(recent_lows)))
        
        # Check lower highs and lower lows (downtrend)
        lower_highs = all(recent_highs[i]['price'] <= recent_highs[i-1]['price'] 
                        for i in range(1, len(recent_highs)))
        lower_lows = all(recent_lows[i]['price'] <= recent_lows[i-1]['price'] 
                      for i in range(1, len(recent_lows)))
        
        # Calculate overall trend direction
        uptrend_confirmation = sum([higher_highs, higher_lows])
        downtrend_confirmation = sum([lower_highs, lower_lows])
        
        swing_threshold = self.params['sideways_threshold'] / 100.0  # Convert to decimal
        
        # Calculate trend strength
        if len(recent_highs) >= 2 and len(recent_lows) >= 2:
            # How much prices have changed over the period
            high_change = (recent_highs[-1]['price'] - recent_highs[0]['price']) / recent_highs[0]['price']
            low_change = (recent_lows[-1]['price'] - recent_lows[0]['price']) / recent_lows[0]['price']
            
            # Strength is the average of high and low changes
            trend_strength = (abs(high_change) + abs(low_change)) / 2
        else:
            trend_strength = 0.0
        
        # Check for sideways market (small price movement)
        if abs(trend_strength) < swing_threshold:
            trend = TrendDirection.SIDEWAYS
        # Determine trend based on swing point sequences
        elif uptrend_confirmation > downtrend_confirmation:
            trend = TrendDirection.UP
        elif downtrend_confirmation > uptrend_confirmation:
            trend = TrendDirection.DOWN
        else:
            # If tied, use the most recent swing comparison
            if recent_highs and recent_lows:
                latest_high_idx = recent_highs[-1]['index']
                latest_low_idx = recent_lows[-1]['index']
                
                if latest_high_idx > latest_low_idx and len(recent_highs) >= 2:
                    # Latest swing is a high
                    trend = TrendDirection.UP if recent_highs[-1]['price'] > recent_highs[-2]['price'] else TrendDirection.DOWN
                elif len(recent_lows) >= 2:
                    # Latest swing is a low
                    trend = TrendDirection.UP if recent_lows[-1]['price'] > recent_lows[-2]['price'] else TrendDirection.DOWN
                else:
                    trend = TrendDirection.UNKNOWN
            else:
                trend = TrendDirection.UNKNOWN
        
        # Check for structure breaks
        structure_breaks = self._detect_structure_breaks(sorted_highs, sorted_lows, trend)
        
        # Normalize trend strength to 0-1 range
        normalized_strength = min(1.0, trend_strength * 10)  # Multiply by 10 to scale up small values
        
        return {
            'trend': trend.value,
            'strength': normalized_strength,
            'swings': {
                'highs': recent_highs,
                'lows': recent_lows
            },
            'structure_breaks': structure_breaks,
            'indicators': {
                'ema_short': ema_short[-10:],  # Last 10 values
                'ema_long': ema_long[-10:],    # Last 10 values
            }
        }
    
    def _analyze_ema_trend(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze trend using Exponential Moving Averages (EMAs)
        
        This uses the relationship between short and long-term EMAs to determine trend:
        - Short EMA above long EMA = Uptrend
        - Short EMA below long EMA = Downtrend
        - EMAs crossing or close together = Potential trend change
        """
        if len(candles) < 50:  # Need enough data for EMAs
            return {
                'trend': TrendDirection.UNKNOWN.value,
                'strength': 0.0,
                'indicators': {},
                'structure_breaks': []
            }
        
        # Extract close prices
        closes = [c.get('close', 0) for c in candles]
        
        # Calculate EMAs
        ema_short = self._calculate_ema(closes, 20)
        ema_long = self._calculate_ema(closes, 50)
        
        # Get latest values
        current_short = ema_short[-1]
        current_long = ema_long[-1]
        
        # Calculate trend direction
        if current_short > current_long:
            trend = TrendDirection.UP
        elif current_short < current_long:
            trend = TrendDirection.DOWN
        else:
            trend = TrendDirection.SIDEWAYS
        
        # Calculate trend strength based on separation between EMAs
        trend_strength = abs(current_short - current_long) / current_long
        
        # Check for potential trend changes (EMA crossing)
        structure_breaks = []
        if len(ema_short) > 5 and len(ema_long) > 5:
            # Check if EMAs recently crossed
            prev_short = ema_short[-6:-1]  # Last 5 values before current
            prev_long = ema_long[-6:-1]
            
            # Detect crossing (short crosses above long = bullish, below = bearish)
            if any(prev_short[i] <= prev_long[i] for i in range(5)) and current_short > current_long:
                structure_breaks.append({
                    'type': 'ema_crossover',
                    'direction': 'bullish',
                    'index': len(candles) - 1,
                    'timestamp': candles[-1].get('timestamp', ''),
                    'price': closes[-1],
                    'strength': trend_strength * 10  # Scale up for visibility
                })
            elif any(prev_short[i] >= prev_long[i] for i in range(5)) and current_short < current_long:
                structure_breaks.append({
                    'type': 'ema_crossover',
                    'direction': 'bearish',
                    'index': len(candles) - 1,
                    'timestamp': candles[-1].get('timestamp', ''),
                    'price': closes[-1],
                    'strength': trend_strength * 10  # Scale up for visibility
                })