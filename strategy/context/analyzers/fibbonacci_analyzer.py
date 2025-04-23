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

        if high_price is None or low_price is None:
            logger.info("Skipping Fibonacci analysis: invalid swing point values")
            return current_context, False

        fib_levels = self.calculate_levels(high_price, low_price)
        current_context.set_fib_levels(fib_levels)

        logger.debug(f"Fibonacci levels updated — Support: {len(fib_levels['support'])}, Resistance: {len(fib_levels['resistance'])}")
        return current_context, True

    def calculate_levels(self, high_price: float, low_price: float) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Fibonacci retracement and extension levels.

        Args:
            high_price: Swing high price
            low_price: Swing low price

        Returns:
            Dictionary with support and resistance levels
        """
        price_range = high_price - low_price
        uptrend = high_price > low_price

        support_levels = []
        resistance_levels = []

        if uptrend:
            for level in self.retracement_levels:
                fib_price = high_price - (price_range * level)
                if fib_price < low_price or fib_price > high_price:
                    continue
                support_levels.append({'price': fib_price, 'level': level, 'type': 'retracement'})
            for ext in self.extension_levels:
                ext_price = low_price + (price_range * ext)
                resistance_levels.append({'price': ext_price, 'level': ext, 'type': 'extension'})
        else:
            for level in self.retracement_levels:
                fib_price = low_price + (price_range * level)
                if fib_price < low_price or fib_price > high_price:
                    continue
                resistance_levels.append({'price': fib_price, 'level': level, 'type': 'retracement'})
            for ext in self.extension_levels:
                ext_price = high_price - (price_range * ext)
                support_levels.append({'price': ext_price, 'level': ext, 'type': 'extension'})

        support_levels = sorted(support_levels, key=lambda x: x['price'], reverse=True)
        resistance_levels = sorted(resistance_levels, key=lambda x: x['price'])

        return {
            'support': support_levels,
            'resistance': resistance_levels
        }
