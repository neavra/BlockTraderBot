from typing import List, Dict, Any, Tuple
import logging

from strategy.context.analyzers.base import BaseAnalyzer
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.models.market_context import MarketContext

logger = logging.getLogger(__name__)

class FibonacciAnalyzer(BaseAnalyzer):
    """
    Analyzer that detects Fibonacci retracement and extension levels
    based on swing high and swing low in the current market context.
    """

    def __init__(self, buffer_percent: float = 0.5):
        self.retracement_levels = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
        self.extension_levels = [1.272, 1.618, 2.0, 2.618]
        self.buffer = buffer_percent / 100.0  # Convert to decimal

    def update_market_context(
        self,
        current_context: MarketContext,
        candles: List[CandleDto]
    ) -> Tuple[MarketContext, bool]:
        """
        Update market context with Fibonacci levels using context swing points.
        If swing points are incomplete, do nothing.

        Returns:
            Tuple of (possibly updated MarketContext, update flag)
        """
        swing_high = current_context.swing_high
        swing_low = current_context.swing_low

        # Only proceed if both swing points exist and are valid
        if not (swing_high and swing_low):
            logger.info("Skipping Fibonacci analysis: missing swing high or swing low")
            return current_context, False

        high_price = swing_high.get("price")
        low_price = swing_low.get("price")
        high_time = swing_high.get("timestamp")
        low_time = swing_low.get("timestamp")

        if high_price is None or low_price is None or high_time is None or low_time is None:
            logger.info("Skipping Fibonacci analysis: invalid swing point values")
            return current_context, False

        # Determine if the trend is uptrend or downtrend based on timestamps
        uptrend = low_time < high_time

        fib_levels = self.analyze(high_price, low_price, uptrend)
        current_context.set_fib_levels(fib_levels)

        logger.debug(f"Fibonacci levels updated for {'uptrend' if uptrend else 'downtrend'}")
        return current_context, True

    def analyze(self, high_price: float, low_price: float, uptrend: bool) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Fibonacci retracement and extension levels.
        
        Args:
            high_price: Swing high price
            low_price: Swing low price
            uptrend: Whether the market is in uptrend
        
        Returns:
            Dictionary with support and resistance levels
        """
        price_range = high_price - low_price

        support_levels = []
        resistance_levels = []

        if uptrend:
            # Uptrend: retracements are potential supports, extensions are potential resistances
            for level in self.retracement_levels:
                fib_price = high_price - (price_range * level)
                if low_price <= fib_price <= high_price:
                    support_levels.append({'price': fib_price, 'level': level, 'type': 'retracement'})
            for ext in self.extension_levels:
                fib_price = high_price + (price_range * ext)
                resistance_levels.append({'price': fib_price, 'level': ext, 'type': 'extension'})
        else:
            # Downtrend: retracements are potential resistances, extensions are potential supports
            for level in self.retracement_levels:
                fib_price = low_price + (price_range * level)
                if low_price <= fib_price <= high_price:
                    resistance_levels.append({'price': fib_price, 'level': level, 'type': 'retracement'})
            for ext in self.extension_levels:
                fib_price = low_price - (price_range * ext)
                support_levels.append({'price': fib_price, 'level': ext, 'type': 'extension'})

        support_levels = sorted(support_levels, key=lambda x: x['price'], reverse=True)
        resistance_levels = sorted(resistance_levels, key=lambda x: x['price'])

        return {
            'support': support_levels,
            'resistance': resistance_levels
        }

