import logging
from typing import List, Dict, Any
from strategy.context.analyzers.base import BaseAnalyzer
from strategy.domain.models.market_context import MarketContext

logger = logging.getLogger(__name__)

class FibonacciAnalyzer(BaseAnalyzer):
    """
    Analyzer that detects Fibonacci retracement and extension levels
    """
    
    def __init__(self, buffer_percent: float = 0.5):
        """
        Initialize the Fibonacci level analyzer
        
        Args:
            buffer_percent: Buffer around levels (%) to avoid levels too close to price
        """
        # Standard Fibonacci retracement levels
        self.retracement_levels = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
        
        # Standard Fibonacci extension levels
        self.extension_levels = [1.272, 1.618, 2.0, 2.618]
        
        # Buffer percentage (convert to decimal)
        self.buffer = buffer_percent / 100.0
    
    def analyze(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze candle data to calculate Fibonacci levels
        
        Args:
            candles: List of candle data
            
        Returns:
            Dictionary with Fibonacci level analysis results
        """
        if not candles:
            logger.warning("No candles provided for Fibonacci analysis")
            return {'support': [], 'resistance': []}
            
        # Calculate high and low from candles
        high_price = max(c.get('high', c.get('close', 0)) for c in candles)
        low_price = min(c.get('low', c.get('close', 0)) for c in candles)
        current_price = candles[-1].get('close', 0)
        
        # Calculate Fibonacci levels
        return self.calculate_levels(high_price, low_price, current_price)
    
    def update_market_context(self, context: MarketContext, candles: List[Dict[str, Any]]):
        """
        Update market context with Fibonacci levels
        
        Args:
            context: MarketContext object to update
            candles: List of candle data
            
        Returns:
            Updated MarketContext
        """
        # Get current price from context or candles
        current_price = context.current_price
        
        if current_price is None and candles:
            current_price = candles[-1].get('close', 0)
        
        if current_price is None:
            logger.error("Current price not available, cannot calculate Fibonacci levels")
            return context
        
        # Try to get swing high/low points from context
        swing_high = context.swing_high
        swing_low = context.swing_low
        
        # Determine high and low prices for Fibonacci calculation
        if swing_high and swing_low and isinstance(swing_high, dict) and isinstance(swing_low, dict):
            high_price = swing_high.get('price')
            low_price = swing_low.get('price')
        else:
            # Calculate from candles if swing points not available
            high_price = max(c.get('high', c.get('close', 0)) for c in candles)
            low_price = min(c.get('low', c.get('close', 0)) for c in candles)
            
        # Calculate Fibonacci levels
        fib_levels = self.calculate_levels(high_price, low_price, current_price)
        
        # Update market context
        context.fib_levels = fib_levels
        
        logger.debug(f"Updated market context with {len(fib_levels['support'])} support and {len(fib_levels['resistance'])} resistance levels")
        
        return context
    
    def calculate_levels(self, high_price: float, low_price: float, current_price: float) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Fibonacci retracement and extension levels
        
        Args:
            high_price: The highest price in the analyzed range
            low_price: The lowest price in the analyzed range
            current_price: The current price
            
        Returns:
            Dictionary with support and resistance levels
        """
        # Price range
        price_range = high_price - low_price
        
        # Determine if we're in an uptrend or downtrend
        uptrend = high_price > low_price
        
        support_levels = []
        resistance_levels = []
        
        # Calculate levels
        if uptrend:
            # In uptrend, retracements are potential support, extensions are potential resistance
            
            # Calculate retracement levels
            for level in self.retracement_levels:
                fib_price = high_price - (price_range * level)
                
                # Skip levels too close to current price
                if abs(fib_price - current_price) / current_price < self.buffer:
                    continue
                
                # Determine if this is support or resistance
                if fib_price < current_price:
                    support_levels.append({
                        'price': fib_price,
                        'level': level,
                        'type': 'retracement'
                    })
                else:
                    resistance_levels.append({
                        'price': fib_price,
                        'level': level,
                        'type': 'retracement'
                    })
            
            # Calculate extension levels as resistance
            for ext in self.extension_levels:
                ext_price = low_price + (price_range * ext)
                
                # Skip levels too close to current price
                if abs(ext_price - current_price) / current_price < self.buffer:
                    continue
                
                resistance_levels.append({
                    'price': ext_price,
                    'level': ext,
                    'type': 'extension'
                })
                
        else:
            # In downtrend, retracements are potential resistance, extensions are potential support
            
            # Calculate retracement levels
            for level in self.retracement_levels:
                fib_price = low_price + (price_range * level)
                
                # Skip levels too close to current price
                if abs(fib_price - current_price) / current_price < self.buffer:
                    continue
                
                # Determine if this is support or resistance
                if fib_price > current_price:
                    resistance_levels.append({
                        'price': fib_price,
                        'level': level,
                        'type': 'retracement'
                    })
                else:
                    support_levels.append({
                        'price': fib_price,
                        'level': level,
                        'type': 'retracement'
                    })
            
            # Calculate extension levels as support
            for ext in self.extension_levels:
                ext_price = high_price - (price_range * ext)
                
                # Skip levels too close to current price
                if abs(ext_price - current_price) / current_price < self.buffer:
                    continue
                
                support_levels.append({
                    'price': ext_price,
                    'level': ext,
                    'type': 'extension'
                })
        
        # Sort levels by price
        support_levels = sorted(support_levels, key=lambda x: x['price'], reverse=True)
        resistance_levels = sorted(resistance_levels, key=lambda x: x['price'])
        
        return {
            'support': support_levels,
            'resistance': resistance_levels
        }