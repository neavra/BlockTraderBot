"""
Candle Aggregator for processing standard candles into custom timeframes.
"""

import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

from shared.domain.dto.candle_dto import CandleDto
from shared.queue.queue_service import QueueService
from shared.constants import Exchanges, RoutingKeys
from managers.state_manager import StateManager
from utils.timeframe_utils import calculate_candle_boundaries, timeframe_to_ms

class CandleAggregator:
    """
    Aggregates standard timeframe candles into custom timeframe candles.
    
    This component:
    1. Processes standard candles according to custom timeframe configurations
    2. Maintains and updates partial custom timeframe candles
    3. Publishes completed custom timeframe candles to the event bus
    """
    
    def __init__(self, state_manager: StateManager, queue_service: QueueService, config: Dict[str, Any]):
        """
        Initialize the candle aggregator.
        
        Args:
            state_manager: Manager for partial candle state
            queue_service: Queue service for publishing completed candles
            config: Configuration dictionary with custom timeframe mappings
        """
        self.state_manager = state_manager
        self.queue_service = queue_service
        self.config = config
        self.logger = logging.getLogger("CandleAggregator")
    
    async def process_candle(self, standard_candle: CandleDto, custom_timeframe: str) -> Optional[CandleDto]:
        """
        Process a standard candle for a specific custom timeframe.
        
        Args:
            standard_candle: Standard timeframe candle
            custom_timeframe: Target custom timeframe
            
        Returns:
            Completed custom timeframe candle if ready, None otherwise
        """
        try:
            # Get timeframe config
            timeframe_config = self.config['data']['custom_timeframes']['mappings'].get(custom_timeframe)
            if not timeframe_config:
                self.logger.warning(f"No configuration found for custom timeframe: {custom_timeframe}")
                return None
            
            # Calculate boundaries for this standard candle's timestamp
            start_time, end_time = calculate_candle_boundaries(
                standard_candle.timestamp, timeframe_config
            )
            
            # Try to get an existing partial candle for this timeframe
            partial_candle = await self.state_manager.get_partial_candle(
                standard_candle.exchange,
                standard_candle.symbol,
                custom_timeframe,
                end_time
            )
            
            # First update for this custom timeframe candle?
            is_first_update = partial_candle is None
            
            if is_first_update:
                # Create a new custom timeframe candle
                partial_candle = CandleDto(
                    #id=None,  # Will be assigned when stored in database
                    symbol=standard_candle.symbol,
                    exchange=standard_candle.exchange,
                    timeframe=custom_timeframe,
                    timestamp=end_time,  # Use end time as the timestamp for the custom candle
                    open=standard_candle.open,
                    high=standard_candle.high,
                    low=standard_candle.low,
                    close=standard_candle.close,
                    volume=standard_candle.volume,
                    is_closed=False,  # Initially not closed
                    raw_data=None  # No raw data for custom timeframes
                )
                
                self.logger.debug(
                    f"Created new partial candle for {standard_candle.exchange}:{standard_candle.symbol} "
                    f"{custom_timeframe} starting at {start_time.isoformat()}"
                )
            else:
                # Merge the new data into the existing partial candle
                partial_candle = self.state_manager.merge_candle(
                    partial_candle, standard_candle, is_first_update=is_first_update
                )
                
                self.logger.debug(
                    f"Updated partial candle for {standard_candle.exchange}:{standard_candle.symbol} "
                    f"{custom_timeframe} opened at {start_time.isoformat()} and closing at {end_time.isoformat()}"
                )
            
            # Determine if the custom candle is now complete
            # Check if the standard candle's end timestamp meets or exceeds the custom candle's end time
            standard_candle_end = standard_candle.timestamp
            """if hasattr(standard_candle, 'is_closed') and standard_candle.is_closed:
                # Add the standard timeframe duration to get the end time
                standard_tf_ms = timeframe_to_ms(standard_candle.timeframe)
                standard_candle_end = datetime.fromtimestamp(
                    standard_candle.timestamp.timestamp() + (standard_tf_ms / 1000), 
                    tz=timezone.utc
                ) """
            
            is_complete = standard_candle_end >= end_time and standard_candle.is_closed
            
            if is_complete:
                # Mark as complete and closed
                completed_candle = CandleDto(
                    id=partial_candle.id,
                    symbol=partial_candle.symbol,
                    exchange=partial_candle.exchange,
                    timeframe=partial_candle.timeframe,
                    timestamp=partial_candle.timestamp,
                    open=partial_candle.open,
                    high=partial_candle.high,
                    low=partial_candle.low,
                    close=partial_candle.close,
                    volume=partial_candle.volume,
                    is_closed=True,  # Mark as closed
                    raw_data=None
                )
                
                self.logger.info(
                    f"Completed candle for {standard_candle.exchange}:{standard_candle.symbol} "
                    f"{custom_timeframe} opened at {start_time.isoformat()} and closed {end_time.isoformat()}"
                )
                
                # Delete the partial candle from the cache
                await self.state_manager.delete_partial_candle(
                    standard_candle.exchange,
                    standard_candle.symbol,
                    custom_timeframe,
                    end_time
                )
                
                # Publish to the event bus
                await self.publish_custom_candle(completed_candle)
                
                return completed_candle
            else:
                # Store the updated partial candle
                await self.state_manager.store_partial_candle(
                    partial_candle, start_time, end_time
                )
                return None
                
        except Exception as e:
            self.logger.error(f"Error processing candle for custom timeframe: {e}")
            return None
    
    async def publish_custom_candle(self, candle: CandleDto) -> bool:
        """
        Publish a custom timeframe candle to the event bus.
        
        Args:
            candle: Completed custom timeframe candle
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            # Create routing key for this candle
            routing_key = RoutingKeys.CANDLE_NEW.format(
                exchange=candle.exchange,
                symbol=candle.symbol,
                timeframe=candle.timeframe
            )
            
            # Convert candle to JSON
            # Note: Need to handle datetime serialization properly
            from data.utils.helper import DateTimeEncoder
            import json
            candle_json = json.dumps(vars(candle), cls=DateTimeEncoder)
            
            # Publish to the event bus
            self.queue_service.publish(
                exchange=Exchanges.MARKET_DATA,
                routing_key=routing_key,
                message=candle_json
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error publishing custom timeframe candle: {e}")
            return False
    
    async def process_candles_batch(self, standard_candles: List[CandleDto], 
                                  custom_timeframes: List[str]) -> List[CandleDto]:
        """
        Process multiple standard candles for multiple custom timeframes.
        
        Args:
            standard_candles: List of standard timeframe candles
            custom_timeframes: List of target custom timeframes
            
        Returns:
            List of completed custom timeframe candles
        """
        completed_candles = []
        
        # Process each standard candle for each custom timeframe
        for candle in standard_candles:
            for timeframe in custom_timeframes:
                completed_candle = await self.process_candle(candle, timeframe)
                if completed_candle:
                    completed_candles.append(completed_candle)
        
        return completed_candles