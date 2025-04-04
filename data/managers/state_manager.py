"""
State Manager for tracking and managing partial custom timeframe candles.
"""

import logging
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

from shared.domain.dto.candle_dto import CandleDto
from shared.cache.cache_service import CacheService
from shared.constants import CacheKeys, CacheTTL

class StateManager:
    """
    Manages the state of partial custom timeframe candles.
    
    This component:
    1. Tracks in-progress (partial) custom timeframe candles
    2. Handles retrieval and storage of partial candles from/to cache
    3. Provides merging utilities for updating partial candles
    """
    
    def __init__(self, cache_service: CacheService):
        """
        Initialize the state manager.
        
        Args:
            cache_service: Cache service for storing partial candles
        """
        self.cache = cache_service
        self.logger = logging.getLogger("StateManager")
    
    def get_partial_candle_key(self, exchange: str, symbol: str, 
                              timeframe: str, end_time: datetime) -> str:
        """
        Generate a cache key for a partial candle.
        
        Args:
            exchange: Exchange name
            symbol: Symbol name
            timeframe: Timeframe string
            end_time: End time of the custom timeframe candle
            
        Returns:
            Cache key string
        """
        timestamp_str = end_time.isoformat()
        return f"partial:candle:{exchange}:{symbol}:{timeframe}:{timestamp_str}"
    
    async def get_partial_candle(self, exchange: str, symbol: str, 
                               timeframe: str, end_time: datetime) -> Optional[CandleDto]:
        """
        Retrieve a partial candle from the cache.
        
        Args:
            exchange: Exchange name
            symbol: Symbol name
            timeframe: Timeframe string
            end_time: End time of the custom timeframe candle
            
        Returns:
            Partial candle DTO if found, None otherwise
        """
        try:
            cache_key = self.get_partial_candle_key(exchange, symbol, timeframe, end_time)
            candle_data = self.cache.get(cache_key)
            
            if not candle_data:
                return None
            
            # Convert from cache data to CandleDto
            # Note: CandleDto should handle datetime conversion in __post_init__
            return CandleDto(**candle_data)
            
        except Exception as e:
            self.logger.error(f"Error retrieving partial candle: {e}")
            return None
    
    async def store_partial_candle(self, candle: CandleDto, 
                                 start_time: datetime, end_time: datetime, 
                                 ttl: int = CacheTTL.DAY) -> bool:
        """
        Store a partial candle in the cache.
        
        Args:
            candle: Candle DTO to store
            start_time: Start time of the custom timeframe candle
            end_time: End time of the custom timeframe candle
            ttl: Time-to-live in seconds
            
        Returns:
            True if stored successfully, False otherwise
        """
        try:
            # Add boundary information to the candle data
            from data.utils.helper import DateTimeEncoder
            import json

            # Get candle as dict and add boundary data
            candle_data = vars(candle)
            #candle_data['start_time'] = start_time
            #candle_data['end_time'] = end_time
            
            # Convert to JSON-serializable dict
            serialized_data = json.loads(json.dumps(candle_data, cls=DateTimeEncoder))
            
            # Store in cache
            cache_key = self.get_partial_candle_key(
                candle.exchange, candle.symbol, candle.timeframe, end_time
            )
            return self.cache.set(cache_key, serialized_data, ttl)
            
        except Exception as e:
            self.logger.error(f"Error storing partial candle: {e}")
            return False
    
    async def delete_partial_candle(self, exchange: str, symbol: str, 
                                  timeframe: str, end_time: datetime) -> bool:
        """
        Delete a partial candle from the cache.
        
        Args:
            exchange: Exchange name
            symbol: Symbol name
            timeframe: Timeframe string
            end_time: End time of the custom timeframe candle
            
        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            cache_key = self.get_partial_candle_key(exchange, symbol, timeframe, end_time)
            return self.cache.delete(cache_key)
            
        except Exception as e:
            self.logger.error(f"Error deleting partial candle: {e}")
            return False
    
    def merge_candle(self, existing_candle: CandleDto, new_candle: CandleDto, 
                    is_first_update: bool = False) -> CandleDto:
        """
        Merge a new candle into an existing partial candle.
        
        Args:
            existing_candle: Existing partial candle
            new_candle: New candle to merge in
            is_first_update: Whether this is the first update for this custom timeframe
            
        Returns:
            Updated candle DTO
        """
        # Create a new candle DTO to avoid modifying the input
        updated_candle = CandleDto(
            id=existing_candle.id,
            symbol=existing_candle.symbol,
            exchange=existing_candle.exchange,
            timeframe=existing_candle.timeframe,
            timestamp=existing_candle.timestamp,
            open=existing_candle.open if not is_first_update else new_candle.open,
            high=max(existing_candle.high, new_candle.high),
            low=min(existing_candle.low, new_candle.low),
            close=new_candle.close,  # Always use the latest close
            volume=existing_candle.volume + new_candle.volume,
            is_closed=existing_candle.is_closed,
            raw_data=None
        )
        
        return updated_candle
    
    async def list_partial_candles(self, exchange: str, symbol: str) -> List[CandleDto]:
        """
        List all partial candles for a symbol.
        
        Args:
            exchange: Exchange name
            symbol: Symbol name
            
        Returns:
            List of partial candle DTOs
        """
        try:
            pattern = f"partial:candle:{exchange}:{symbol}:*"
            keys = self.cache.keys(pattern)
            
            candles = []
            for key in keys:
                candle_data = self.cache.get(key)
                if candle_data:
                    candles.append(CandleDto(**candle_data))
            
            return candles
            
        except Exception as e:
            self.logger.error(f"Error listing partial candles: {e}")
            return []
    
    async def cleanup_expired_candles(self) -> int:
        """
        Cleanup expired partial candles from the cache.
        
        Returns:
            Number of expired candles cleaned up
        """
        # This should be a no-op with Redis TTL, but can be implemented
        # for in-memory caches or additional cleanup logic
        return 0