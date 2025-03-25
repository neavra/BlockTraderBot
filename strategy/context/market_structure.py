# strategy/context/market_structure.py
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
import time
from datetime import datetime

from strategy.context.trend_analysis import TrendAnalyzer, TrendDirection
from strategy.context.swing_detector import SwingDetector
from strategy.context.levels import SupportResistanceDetector

logger = logging.getLogger(__name__)

@dataclass
class SwingPoint:
    """Represents a significant swing point in the market"""
    price: float
    timestamp: str
    type: str  # "high" or "low"
    index: int  # Index in the candle array
    broken: bool = False
    strength: float = 1.0  # 0.0 to 1.0

@dataclass
class TrendState:
    """Represents the current trend state"""
    direction: TrendDirection = TrendDirection.UNKNOWN
    strength: float = 0.0  # 0.0 to 1.0
    swing_high: Optional[SwingPoint] = None
    swing_low: Optional[SwingPoint] = None
    last_break: Optional[Dict[str, Any]] = None

@dataclass
class MarketState:
    """Represents the complete state of the market"""
    symbol: str
    timeframe: str
    timestamp: str
    trend: TrendState
    swings: Dict[str, List[SwingPoint]]
    levels: Dict[str, List[Dict[str, Any]]]
    structure_breaks: List[Dict[str, Any]]
    last_updated: float  # Unix timestamp

class MarketStructure:
    """
    Main class for tracking and analyzing market structure across timeframes
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize the market structure analyzer
        
        Args:
            params: Configuration parameters
        """
        default_params = {
            'swing_detection': {
                'method': 'zigzag',
                'lookback': 5,
                'threshold': 0.5,  # % change
                'min_swing_separation': 3
            },
            'trend_analysis': {
                'method': 'swing',
                'lookback': 4,
                'threshold': 1.0,  # % threshold for trend changes
            },
            'level_detection': {
                'method': 'all',
                'num_levels': 5,
                'threshold': 0.3,
                'zone_size': 0.2
            }
        }
        
        if params:
            # Deep merge params
            for section, values in params.items():
                if section in default_params and isinstance(values, dict):
                    default_params[section].update(values)
                else:
                    default_params[section] = values
        
        self.params = default_params
        
        # Initialize component analyzers
        self.swing_detector = SwingDetector(self.params.get('swing_detection', {}))
        self.trend_analyzer = TrendAnalyzer(self.params.get('trend_analysis', {}))
        self.level_detector = SupportResistanceDetector(self.params.get('level_detection', {}))
        
        # State storage for each symbol and timeframe
        self.market_states = {}
    
    def get_state(self, symbol: str, timeframe: str) -> Optional[MarketState]:
        """
        Get the current market state for a symbol and timeframe
        
        Args:
            symbol: Trading pair
            timeframe: Candle timeframe
            
        Returns:
            Market state or None if not available
        """
        key = f"{symbol}_{timeframe}"
        return self.market_states.get(key)
    
    async def update(self, data: Dict[str, Any]) -> Optional[MarketState]:
        """
        Update market structure based on new candle data
        
        Args:
            data: Market data dictionary with candles
            
        Returns:
            Updated market state
        """
        symbol = data.get('symbol')
        timeframe = data.get('timeframe')
        candles = data.get('candles', [])
        timestamp = data.get('timestamp', datetime.now().isoformat())
        
        if not symbol or not timeframe or not candles or len(candles) < 10:
            logger.warning(f"Insufficient data to update market structure: {symbol} {timeframe}")
            return None
        
        # Get existing state or create new one
        key = f"{symbol}_{timeframe}"
        state = self.market_states.get(key)
        
        # Create new state if doesn't exist
        if not state:
            state = MarketState(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=timestamp,
                trend=TrendState(),
                swings={
                    'highs': [],
                    'lows': []
                },
                levels={
                    'support': [],
                    'resistance': []
                },
                structure_breaks=[],
                last_updated=time.time()
            )
        
        # Update timestamp
        state.timestamp = timestamp
        
        # Step 1: Detect swing points
        swings = self.swing_detector.detect_swings(candles)
        
        # Convert to SwingPoint objects
        swing_highs = [
            SwingPoint(
                price=h['price'],
                timestamp=h['timestamp'],
                type='high',
                index=h['index'],
                strength=h.get('strength', 0.5)
            ) for h in swings.get('highs', [])
        ]
        
        swing_lows = [
            SwingPoint(
                price=l['price'],
                timestamp=l['timestamp'],
                type='low',
                index=l['index'],
                strength=l.get('strength', 0.5)
            ) for l in swings.get('lows', [])
        ]
        
        # Update state with new swing points
        state.swings = {
            'highs': swing_highs,
            'lows': swing_lows
        }
        
        # Step 2: Analyze trend
        trend_results = self.trend_analyzer.analyze_trend(candles, swings)
        
        # Update trend state
        state.trend.direction = self._parse_trend_direction(trend_results.get('trend', TrendDirection.UNKNOWN.value))
        state.trend.strength = trend_results.get('strength', 0.0)
        
        # Update structure breaks
        structure_breaks = trend_results.get('structure_breaks', [])
        if structure_breaks:
            state.structure_breaks = structure_breaks
            state.trend.last_break = structure_breaks[0] if structure_breaks else None
        
        # Step 3: Detect support/resistance levels
        levels = self.level_detector.detect_levels(candles, swings)
        state.levels = levels
        
        # Step 4: Update latest swing high/low references
        if swing_highs:
            latest_high = max(swing_highs, key=lambda x: x.index)
            state.trend.swing_high = latest_high
            
        if swing_lows:
            latest_low = max(swing_lows, key=lambda x: x.index)
            state.trend.swing_low = latest_low
        
        # Save updated state
        state.last_updated = time.time()
        self.market_states[key] = state
        
        return state
    
    def analyze_multi_timeframe(self, symbol: str, timeframes: List[str]) -> Dict[str, Any]:
        """
        Perform cross-timeframe analysis
        
        Args:
            symbol: Trading pair
            timeframes: List of timeframes sorted from shortest to longest
            
        Returns:
            Dictionary with cross-timeframe analysis
        """
        results = {}
        trend_alignment = 0
        trend_count = 0
        
        # Analyze each timeframe
        for tf in timeframes:
            state = self.get_state(symbol, tf)
            if not state:
                continue
                
            results[tf] = {
                'trend': state.trend.direction.value,
                'strength': state.trend.strength,
                'last_break': state.trend.last_break,
                'has_recent_break': bool(state.structure_breaks) and \
                                    self._is_break_recent(state.structure_breaks[0], tf) if state.structure_breaks else False
            }
            
            # Count aligned timeframes
            if state.trend.direction == TrendDirection.UP:
                trend_alignment += 1
                trend_count += 1
            elif state.trend.direction == TrendDirection.DOWN:
                trend_alignment -= 1
                trend_count += 1
            else:
                trend_count += 1
        
        # Higher timeframe bias (if available)
        higher_tf_bias = None
        higher_tf_strength = 0
        
        if timeframes and results:
            for tf in reversed(timeframes):  # Start from highest timeframe
                if tf in results:
                    higher_tf_bias = results[tf]['trend']
                    higher_tf_strength = results[tf]['strength']
                    break
        
        # Calculate alignment metrics
        if trend_count > 0:
            alignment_strength = abs(trend_alignment) / trend_count
            alignment_direction = "bullish" if trend_alignment > 0 else "bearish" if trend_alignment < 0 else "neutral"
        else:
            alignment_strength = 0
            alignment_direction = "unknown"
        
        return {
            'timeframes': results,
            'alignment': {
                'direction': alignment_direction,
                'strength': alignment_strength,
                'score': trend_alignment / max(1, trend_count)  # -1 to 1 range
            },
            'higher_timeframe_bias': higher_tf_bias,
            'higher_timeframe_strength': higher_tf_strength
        }
    
    def detect_pullbacks(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        """
        Detect pullbacks in the current trend
        
        A pullback is a temporary price movement against the trend
        
        Args:
            symbol: Trading pair
            timeframe: Timeframe to analyze
            
        Returns:
            List of detected pullbacks
        """
        state = self.get_state(symbol, timeframe)
        if not state:
            return []
        
        # Trend direction
        trend = state.trend.direction
        
        # Need defined trend
        if trend == TrendDirection.UNKNOWN or trend == TrendDirection.SIDEWAYS:
            return []
        
        pullbacks = []
        
        # In uptrend, look for pullbacks to support levels
        if trend == TrendDirection.UP:
            support_levels = state.levels.get('support', [])
            
            for level in support_levels:
                # Check if price is currently near this level
                current_price = state.last_price if hasattr(state, 'last_price') else None
                if not current_price:
                    continue
                
                # Check if price is within the support zone
                if level['zone'][0] <= current_price <= level['zone'][1]:
                    pullbacks.append({
                        'type': 'support_pullback',
                        'level': level['price'],
                        'strength': level['strength'],
                        'current_price': current_price,
                        'trend': trend.value
                    })
        
        # In downtrend, look for pullbacks to resistance levels
        elif trend == TrendDirection.DOWN:
            resistance_levels = state.levels.get('resistance', [])
            
            for level in resistance_levels:
                # Check if price is currently near this level
                current_price = state.last_price if hasattr(state, 'last_price') else None
                if not current_price:
                    continue
                
                # Check if price is within the resistance zone
                if level['zone'][0] <= current_price <= level['zone'][1]:
                    pullbacks.append({
                        'type': 'resistance_pullback',
                        'level': level['price'],
                        'strength': level['strength'],
                        'current_price': current_price,
                        'trend': trend.value
                    })
        
        return pullbacks
    
    def _parse_trend_direction(self, trend_str: str) -> TrendDirection:
        """Convert string trend direction to enum"""
        if trend_str == "uptrend":
            return TrendDirection.UP
        elif trend_str == "downtrend":
            return TrendDirection.DOWN
        elif trend_str == "sideways":
            return TrendDirection.SIDEWAYS
        else:
            return TrendDirection.UNKNOWN
    
    def _is_break_recent(self, break_data: Dict[str, Any], timeframe: str) -> bool:
        """Check if a structure break is recent based on timeframe"""
        if not break_data or 'index' not in break_data:
            return False
            
        # Set thresholds for "recent" based on timeframe
        thresholds = {
            '1m': 20,
            '5m': 12,
            '15m': 8,
            '30m': 6,
            '1h': 4,
            '4h': 3,
            '1d': 2
        }
        
        # Default if timeframe not in thresholds
        threshold = thresholds.get(timeframe, 5)
        
        # Get candle index of the break
        break_index = break_data['index']
        
        # Check if break happened within the threshold
        return break_index >= (threshold * -1)