from typing import List, Dict, Any, Optional, Tuple
import logging

from strategy.context.analyzers.base import BaseAnalyzer
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.models.market_context import MarketContext

logger = logging.getLogger(__name__)

class SwingDetector(BaseAnalyzer):
    """
    Swing detector that identifies the most recent confirmed swing highs and lows,
    and updates the market context if they are newer or more extreme than previous ones.
    """

    def __init__(self, lookback: int = 5):
        """
        Args:
            lookback: Minimum number of candles required to begin swing detection
        """
        self.lookback = lookback

    def analyze(self, candles: List[CandleDto]) -> Dict[str, Optional[Dict[str, Any]]]:
        if len(candles) < self.lookback:
            logger.warning(f"Not enough candles to detect swings (required: {self.lookback})")
            return {"swing_high": None, "swing_low": None}

        highest_swing_high = None
        lowest_swing_low = None
        candidate = {}
        for i in range(1, len(candles) - 1):
            prev = candles[i - 1]
            curr = candles[i]
            next = candles[i + 1]
            # Confirmed swing high
            if curr.high > prev.high and curr.high > next.high:
                candidate = {
                    "price": curr.high,
                    "index": i,
                    "timestamp": curr.timestamp,
                }

                if (
                    not highest_swing_high
                    or candidate["price"] > highest_swing_high["price"]
                    or (
                        candidate["price"] == highest_swing_high["price"]
                        and candidate["index"] > highest_swing_high["index"]
                    )
                ):
                    highest_swing_high = candidate

            # Confirmed swing low
            if curr.low < prev.low and curr.low < next.low:
                candidate = {
                    "price": curr.low,
                "index": i,
                "timestamp": curr.timestamp,
            }

            if (
                not lowest_swing_low
                or candidate["price"] < lowest_swing_low["price"]
                or (
                    candidate["price"] == lowest_swing_low["price"]
                    and candidate["index"] > lowest_swing_low["index"]
                )
            ):
                lowest_swing_low = candidate

        return {
            "swing_high": highest_swing_high,
            "swing_low": lowest_swing_low
        }


    def update_market_context(
        self,
        current_context: MarketContext,
        candles: List[CandleDto]
    ) -> Tuple[MarketContext, bool]:
        """
        Update the MarketContext with new swing high/low if newer or more extreme.
        Ensures that if one swing side is missing, the other still updates without clearing the missing side.
        
        Returns:
            Tuple of (Updated MarketContext, update flag)
        """
        swings = self.analyze(candles)
        updated = False

        new_high = swings["swing_high"]
        old_high = current_context.swing_high
        new_low = swings["swing_low"]
        old_low = current_context.swing_low

        if new_high:
            if (not old_high) or (
                new_high["price"] >= old_high["price"] and new_high["timestamp"] > old_high["timestamp"]
            ):
                current_context.set_swing_high(new_high)
                updated = True
            else:
                current_context.set_swing_high(old_high)
        elif old_high:
            current_context.set_swing_high(old_high)

        # Handle Swing Low
        if new_low:
            if (not old_low) or (
                new_low["price"] <= old_low["price"] and new_low["timestamp"] > old_low["timestamp"]
            ):
                current_context.set_swing_low(new_low)
                updated = True
            else:
                current_context.set_swing_low(old_low)
        elif old_low:
            current_context.set_swing_low(old_low)

        return current_context, updated

