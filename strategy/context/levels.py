import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class SimpleFibonacciLevels:
    """
    A simplified detector for Fibonacci retracement and extension levels
    """
    
    def __init__(self, buffer_percent: float = 0.5):
        """
        Initialize the Fibonacci level detector
        
        Args:
            buffer_percent: Buffer around levels (%) to avoid levels too close to price
        """
        # Standard Fibonacci retracement levels
        self.retracement_levels = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
        
        # Standard Fibonacci extension levels
        self.extension_levels = [1.272, 1.618, 2.0, 2.618]
        
        # Buffer percentage (convert to decimal)
        self.buffer = buffer_percent / 100.0
        
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
    
    def update_market_context(self, market_context: Dict[str, Any], 
                             candles: List[Dict[str, Any]] = None,
                             high_price: float = None, 
                             low_price: float = None) -> Dict[str, Any]:
        """
        Update market context with Fibonacci levels
        
        Args:
            market_context: The current market context
            candles: Optional list of candles to calculate high/low from (if high_price/low_price not provided)
            high_price: The highest price in the analyzed range (optional if candles provided)
            low_price: The lowest price in the analyzed range (optional if candles provided)
            
        Returns:
            Updated market context with Fibonacci levels
        """
        # Get current price from context or candles
        current_price = market_context.get('current_price')
        
        if current_price is None and candles:
            current_price = candles[-1].get('close', 0)
        
        if current_price is None:
            logger.error("Current price not available, cannot calculate Fibonacci levels")
            return market_context
        
        # Get high and low prices if not provided
        if high_price is None or low_price is None:
            # Try to get from swing high/low in context
            swing_high = market_context.get('swing_high')
            swing_low = market_context.get('swing_low')
            
            if swing_high and swing_low:
                high_price = swing_high.get('price')
                low_price = swing_low.get('price')
            elif candles:
                # Calculate from candles
                high_price = max(c.get('high', c.get('close', 0)) for c in candles)
                low_price = min(c.get('low', c.get('close', 0)) for c in candles)
            else:
                logger.error("No high/low prices or candles provided, cannot calculate Fibonacci levels")
                return market_context
        
        # Calculate Fibonacci levels
        fib_levels = self.calculate_levels(high_price, low_price, current_price)
        
        # Update market context
        market_context['fib_levels'] = fib_levels
        
        return market_context