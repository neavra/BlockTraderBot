import logging
from typing import Dict, List, Any, Optional
from enum import Enum
from dataclasses import dataclass
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class TrendDirection(Enum):
    UP = "uptrend"
    DOWN = "downtrend"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"

@dataclass
class SwingPoint:
    """Represents a swing high or low point in the market"""
    price: float
    timestamp: str
    index: int       # Index in the candle array
    type: str        # "high" or "low"
    strength: float = 1.0  # 0.0 to 1.0


class MarketContext:
    """
    Simplified market context object that tracks key market structure elements:
    - Swing highs and lows
    - Trend direction
    - Fibonacci levels
    """
    
    def __init__(self, symbol: str, timeframe: str):
        """
        Initialize market context
        
        Args:
            symbol: Trading pair
            timeframe: Candle timeframe
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.timestamp = None
        self.current_price = None
        
        # Swing points
        self.swing_high = None
        self.swing_low = None
        self.swing_high_history = []
        self.swing_low_history = []
        
        # Trend information
        self.trend = TrendDirection.UNKNOWN.value
        
        # Fibonacci levels
        self.fib_levels = {
            'support': [],
            'resistance': []
        }
        
        # Metadata
        self.last_updated = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert market context to dictionary
        
        Returns:
            Dictionary representation of market context
        """
        return {
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'timestamp': self.timestamp,
            'current_price': self.current_price,
            'swing_high': self._swing_point_to_dict(self.swing_high),
            'swing_low': self._swing_point_to_dict(self.swing_low),
            'swing_high_history': [self._swing_point_to_dict(p) for p in self.swing_high_history],
            'swing_low_history': [self._swing_point_to_dict(p) for p in self.swing_low_history],
            'trend': self.trend,
            'fib_levels': self.fib_levels,
            'last_updated': self.last_updated
        }
    
    def _swing_point_to_dict(self, point: SwingPoint) -> Optional[Dict[str, Any]]:
        """Convert SwingPoint to dictionary"""
        if point is None:
            return None
        return {
            'price': point.price,
            'timestamp': point.timestamp,
            'index': point.index,
            'type': point.type,
            'strength': point.strength
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MarketContext':
        """
        Create MarketContext from dictionary
        
        Args:
            data: Dictionary representation of market context
            
        Returns:
            MarketContext object
        """
        context = cls(data['symbol'], data['timeframe'])
        context.timestamp = data.get('timestamp')
        context.current_price = data.get('current_price')
        
        # Convert swing point dictionaries to SwingPoint objects
        swing_high_dict = data.get('swing_high')
        if swing_high_dict:
            context.swing_high = SwingPoint(
                price=swing_high_dict['price'],
                timestamp=swing_high_dict['timestamp'],
                index=swing_high_dict['index'],
                type=swing_high_dict['type'],
                strength=swing_high_dict.get('strength', 1.0)
            )
        
        swing_low_dict = data.get('swing_low')
        if swing_low_dict:
            context.swing_low = SwingPoint(
                price=swing_low_dict['price'],
                timestamp=swing_low_dict['timestamp'],
                index=swing_low_dict['index'],
                type=swing_low_dict['type'],
                strength=swing_low_dict.get('strength', 1.0)
            )
        
        # Convert swing history
        swing_high_history = data.get('swing_high_history', [])
        context.swing_high_history = [
            SwingPoint(
                price=h['price'],
                timestamp=h['timestamp'],
                index=h['index'],
                type=h['type'],
                strength=h.get('strength', 1.0)
            ) for h in swing_high_history if h is not None
        ]
        
        swing_low_history = data.get('swing_low_history', [])
        context.swing_low_history = [
            SwingPoint(
                price=l['price'],
                timestamp=l['timestamp'],
                index=l['index'],
                type=l['type'],
                strength=l.get('strength', 1.0)
            ) for l in swing_low_history if l is not None
        ]
        
        # Set other fields
        context.trend = data.get('trend', TrendDirection.UNKNOWN.value)
        context.fib_levels = data.get('fib_levels', {'support': [], 'resistance': []})
        context.last_updated = data.get('last_updated', time.time())
        
        return context


class MarketStructure:
    """
    Manages market structure objects for different symbols and timeframes
    """
    
    def __init__(self):
        """Initialize the market structure manager"""
        # Dictionary to store market contexts by symbol and timeframe
        self.contexts = {}
        
        # Initialize analyzers
        self.swing_detector = None
        self.trend_analyzer = None
        self.fib_detector = None
    
    def set_analyzers(self, swing_detector, trend_analyzer, fib_detector):
        """
        Set the analyzers to use for market structure updates
        
        Args:
            swing_detector: Swing detector implementation
            trend_analyzer: Trend analyzer implementation
            fib_detector: Fibonacci level detector implementation
        """
        self.swing_detector = swing_detector
        self.trend_analyzer = trend_analyzer
        self.fib_detector = fib_detector
    
    def get_context(self, symbol: str, timeframe: str) -> Optional[MarketContext]:
        """
        Get market context for a specific symbol and timeframe
        
        Args:
            symbol: Trading pair
            timeframe: Candle timeframe
            
        Returns:
            MarketContext object or None if not found
        """
        key = f"{symbol}_{timeframe}"
        return self.contexts.get(key)
    
    def update_context(self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]) -> MarketContext:
        """
        Update or create market context for a symbol and timeframe
        
        Args:
            symbol: Trading pair
            timeframe: Candle timeframe
            candles: List of candle dictionaries
            
        Returns:
            Updated MarketContext object
        """
        if not candles:
            logger.warning(f"No candles provided for {symbol} {timeframe}")
            return None
        
        # Get existing context or create new one
        key = f"{symbol}_{timeframe}"
        context = self.contexts.get(key)
        
        if not context:
            context = MarketContext(symbol, timeframe)
            self.contexts[key] = context
        
        # Update timestamp and current price
        context.timestamp = datetime.now().isoformat()
        context.current_price = candles[-1].get('close')
        
        # Step 1: Update swing points if detector is available
        if self.swing_detector:
            context = self.swing_detector.update_market_context(context, candles)
        
        # Step 2: Update trend if analyzer is available
        if self.trend_analyzer:
            context = self.trend_analyzer.update_market_context(context)
        
        # Step 3: Update Fibonacci levels if detector is available
        if self.fib_detector:
            context = self.fib_detector.update_market_context(context, candles)
        
        # Update last updated timestamp
        context.last_updated = time.time()
        
        return context
    
    def is_near_level(self, context: MarketContext, price: float, level_type: str = 'all', tolerance: float = 0.005) -> bool:
        """
        Check if a price is near any Fibonacci level
        
        Args:
            context: MarketContext object
            price: Price to check
            level_type: Type of level to check ('support', 'resistance', or 'all')
            tolerance: Price tolerance as percentage
            
        Returns:
            True if price is near a level, False otherwise
        """
        if not context or not context.fib_levels:
            return False
        
        levels_to_check = []
        
        if level_type == 'support' or level_type == 'all':
            levels_to_check.extend(context.fib_levels.get('support', []))
        
        if level_type == 'resistance' or level_type == 'all':
            levels_to_check.extend(context.fib_levels.get('resistance', []))
        
        # Check each level
        for level in levels_to_check:
            level_price = level.get('price')
            if level_price:
                # Calculate tolerance range
                tolerance_range = level_price * tolerance
                
                # Check if price is within tolerance range
                if abs(price - level_price) <= tolerance_range:
                    return True
        
        return False
    
    def detect_order_blocks(self, context: MarketContext, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect potential order blocks based on market structure
        
        An order block is often formed after a swing point is created
        
        Args:
            context: MarketContext object
            candles: List of candle dictionaries
            
        Returns:
            List of potential order blocks
        """
        if not context or not candles:
            return []
        
        # Check for swing highs and lows
        swing_high = context.swing_high
        swing_low = context.swing_low
        
        order_blocks = []
        
        # Look for demand order blocks (support)
        if swing_low and context.trend == TrendDirection.UP.value:
            # Find candle that created the swing low
            swing_index = swing_low.index
            if 0 <= swing_index < len(candles):
                # Order block is typically formed by the candle before the swing
                ob_index = max(0, swing_index - 1)
                
                order_blocks.append({
                    'type': 'demand',  # Buying pressure/support
                    'price_high': candles[ob_index].get('open', candles[ob_index].get('high', 0)),
                    'price_low': candles[ob_index].get('close', candles[ob_index].get('low', 0)),
                    'index': ob_index,
                    'timestamp': candles[ob_index].get('timestamp', ''),
                    'related_swing': self._swing_point_to_dict(swing_low)
                })
        
        # Look for supply order blocks (resistance)
        if swing_high and context.trend == TrendDirection.DOWN.value:
            # Find candle that created the swing high
            swing_index = swing_high.index
            if 0 <= swing_index < len(candles):
                # Order block is typically formed by the candle before the swing
                ob_index = max(0, swing_index - 1)
                
                order_blocks.append({
                    'type': 'supply',  # Selling pressure/resistance
                    'price_high': candles[ob_index].get('open', candles[ob_index].get('high', 0)),
                    'price_low': candles[ob_index].get('close', candles[ob_index].get('low', 0)),
                    'index': ob_index,
                    'timestamp': candles[ob_index].get('timestamp', ''),
                    'related_swing': self._swing_point_to_dict(swing_high)
                })
        
        return order_blocks
    
    def _swing_point_to_dict(self, point: SwingPoint) -> Optional[Dict[str, Any]]:
        """Convert SwingPoint to dictionary"""
        if point is None:
            return None
        return {
            'price': point.price,
            'timestamp': point.timestamp,
            'index': point.index,
            'type': point.type,
            'strength': point.strength
        }