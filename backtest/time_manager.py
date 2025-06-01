import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class TimeManager:
    """
    Manages time progression during backtesting to ensure deterministic execution.
    """
    
    def __init__(self):
        """Initialize the time manager."""
        self.current_time: Optional[datetime] = None
        self.candle_index: int = 0
        self.total_candles: int = 0
        self.logger = logging.getLogger("TimeManager")
    
    def set_current_time(self, timestamp: datetime) -> None:
        """
        Set the current simulation time.
        
        Args:
            timestamp: Current timestamp in the simulation
        """
        self.current_time = timestamp
        self.logger.debug(f"Time set to: {timestamp}")
    
    def advance_to_next_candle(self) -> None:
        """Advance to the next candle in the sequence."""
        self.candle_index += 1
        self.logger.debug(f"Advanced to candle {self.candle_index}/{self.total_candles}")
    
    def set_total_candles(self, total: int) -> None:
        """Set the total number of candles for progress tracking."""
        self.total_candles = total
    
    def get_progress(self) -> float:
        """Get the current progress as a percentage."""
        if self.total_candles == 0:
            return 0.0
        return (self.candle_index / self.total_candles) * 100.0