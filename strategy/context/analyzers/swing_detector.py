from typing import List, Dict, Any, Optional, Tuple
import logging
import copy

from strategy.context.analyzers.base import BaseAnalyzer
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.models.market_context import MarketContext
from datetime import timedelta, datetime, timezone

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
        # TODO move to config in the future
        self.max_candles_age = 100

    def calculate_expiry_time(self, timestamp: datetime, timeframe: str) -> datetime:
        """Calculate when a swing point should expire"""
        timeframe_seconds = self.timeframe_to_seconds(timeframe)
        expiry_seconds = timeframe_seconds * self.max_candles_age
        return timestamp + timedelta(seconds=expiry_seconds)

    def analyze(self, candles: List[CandleDto]) -> Dict[str, Optional[Dict[str, Any]]]:
        if len(candles) < self.lookback:
            logger.warning(f"Not enough candles to detect swings (required: {self.lookback})")
            return {"swing_high": None, "swing_low": None}

        # Get current time and timeframe for expiry calculation
        current_candle = candles[-1]
        timeframe = current_candle.timeframe

        highest_swing_high = None
        lowest_swing_low = None
        
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
                    "expiry": self.calculate_expiry_time(curr.timestamp, timeframe)
                }

                if (
                    not highest_swing_high
                    or candidate["price"] > highest_swing_high["price"]
                    or (
                        candidate["price"] == highest_swing_high["price"]
                        and candidate["timestamp"] > highest_swing_high["timestamp"]
                    )
                ):
                    highest_swing_high = candidate

            # Confirmed swing low
            if curr.low < prev.low and curr.low < next.low:
                candidate = {
                    "price": curr.low,
                    "index": i,
                    "timestamp": curr.timestamp,
                    "expiry": self.calculate_expiry_time(curr.timestamp, timeframe)
                }

                if (
                    not lowest_swing_low
                    or candidate["price"] < lowest_swing_low["price"]
                    or (
                        candidate["price"] == lowest_swing_low["price"]
                        and candidate["timestamp"] > lowest_swing_low["timestamp"]
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
        Handles expiry properly with null checks.
        
        Returns:
            Tuple of (Updated MarketContext, update flag)
        """
        swings = self.analyze(candles)
        updated = False

        new_high = swings["swing_high"]
        old_high = current_context.swing_high
        new_low = swings["swing_low"]
        old_low = current_context.swing_low

        # Get current time for expiry checks
        current_time = candles[-1].timestamp if candles else datetime.now(timezone.utc)

        # ===== HANDLE SWING HIGH =====
        
        # Check if old high has expired
        if old_high and self._is_expired(old_high, current_time):
            old_high = None
            updated = True

        # Update swing high logic
        if new_high:
            should_update = (
                not old_high or  # No existing high
                (new_high["price"] > old_high["price"]) or  # New high is higher
                (new_high["price"] >= old_high["price"] and new_high["timestamp"] > old_high["timestamp"])  # Same/similar price but newer
            )
            
            if should_update:
                current_context.set_swing_high(new_high)
                updated = True
                logger.debug(f"Updated swing high to {new_high['price']} at {new_high['timestamp']}")
            else:
                current_context.set_swing_high(old_high)
        else:
            # No new high found, keep old one if it exists and hasn't expired
            current_context.set_swing_high(old_high)

        # ===== HANDLE SWING LOW =====
        
        # Check if old low has expired
        if old_low and self._is_expired(old_low, current_time):
            old_low = None
            updated = True

        # Update swing low logic
        if new_low:
            should_update = (
                not old_low or  # No existing low
                (new_low["price"] < old_low["price"]) or  # New low is lower
                (new_low["price"] <= old_low["price"] and new_low["timestamp"] > old_low["timestamp"])  # Same/similar price but newer
            )
            
            if should_update:
                current_context.set_swing_low(new_low)
                updated = True
                logger.debug(f"Updated swing low to {new_low['price']} at {new_low['timestamp']}")
            else:
                current_context.set_swing_low(old_low)
        else:
            # No new low found, keep old one if it exists and hasn't expired
            current_context.set_swing_low(old_low)

        return current_context, updated
    

    def _is_expired(self, swing: Dict[str, Any], current_time: datetime) -> bool:
        """
        Check if a swing point has expired
        
        Args:
            swing: Swing point dictionary with 'expiry' field
            current_time: Current timestamp
            
        Returns:
            True if expired, False otherwise
        """
        if not swing or "expiry" not in swing:
            return True
            
        try:
            expiry_time = swing["expiry"]
            if isinstance(expiry_time, str):
                expiry_time = datetime.fromisoformat(expiry_time.replace('Z', '+00:00'))
            
            return current_time >= expiry_time
        except Exception as e:
            logger.warning(f"Error checking swing expiry: {e}")
            return True
        
    @staticmethod
    def timeframe_to_seconds(timeframe: str) -> int:
        """
        Convert timeframe string (e.g., "1h", "4h", "1d") to seconds.
        
        Args:
            timeframe: Timeframe string
            
        Returns:
            Timeframe duration in seconds
        """
        multipliers = {"M": 60, "H": 3600, "D": 86400}
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        return value * multipliers.get(unit, 0)

