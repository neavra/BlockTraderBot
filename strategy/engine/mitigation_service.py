import logging
import asyncio
from typing import Dict, List, Any, Type, Optional, Tuple
from datetime import datetime, timezone, timedelta

from strategy.indicators.base import Indicator
from strategy.domain.types.indicator_type_enum import IndicatorType
from data.database.repository.base_repository import BaseRepository
from shared.domain.dto.candle_dto import CandleDto

logger = logging.getLogger(__name__)

class MitigationService:
    """
    Service responsible for processing mitigation of indicators.
    This service periodically checks active indicators to see if they
    have been mitigated by recent price action.
    """
    
    def __init__(self):
        """Initialize the mitigation service."""
        self.indicators : Dict[str, Indicator] = {}  # Map of indicator_type -> indicator
        self.last_check_times = {}  # Map of indicator_type:symbol:timeframe -> timestamp
        self.running = False
    
    def register_indicator(
        self, 
        indicator_type: IndicatorType,
        indicator: Indicator, 
    ) -> None:
        """
        Register an indicator to be processed for mitigation.
        Only registers if the indicator requires mitigation.
        
        Args:
            indicator_type: Enum value of the indicator type
            indicator: Indicator instance that implements the Indicator interface
            repository: Repository for storing/retrieving indicator instances
        """
        if indicator_type.requires_mitigation:
            self.indicators[indicator_type] = indicator
            logger.info(f"Registered indicator '{indicator_type}' for mitigation processing")
        else:
            logger.debug(f"Indicator '{indicator_type}' does not require mitigation, skipping registration")
    
    async def process_mitigation(
        self,
        candles: List[CandleDto],
    ) -> Dict[str, Any]:
        """
        Process mitigation for all registered indicators for a symbol/timeframe.
        
        Args:
            candles: List of recent candles
            
        Returns:
            Dictionary with mitigation results
        """
        if not candles:
            logger.info("No candles passed into process_mitigation")
            return {}
        
        exchange = candles[0].exchange
        symbol = candles[0].symbol
        timeframe = candles[0].timeframe

        results = {}
        
        # Process each registered indicator
        for indicator_type, indicator in self.indicators.items():
            try:
                # Get relevant price range
                min_price, max_price = indicator.get_relevant_price_range(candles)
                repository = indicator.repository
                # Get active instances in the price range
                if hasattr(repository, 'find_active_instances_in_price_range'):
                    # TODO currently returns a dictionary
                    instances = await indicator.repository.find_active_indicators_in_price_range(
                        exchange=exchange,
                        symbol=symbol,
                        min_price=min_price,
                        max_price=max_price,
                        timeframes=[timeframe]
                    )
                else:
                    logger.error(f"find_active_indicators_in_price_range method not found in repository, indicator type = {indicator_type}")
                
                if not instances:
                    # No active instances to process
                    results[indicator_type] = {
                        "processed": 0,
                        "updated": 0,
                        "mitigated": 0,
                        "still_valid": 0
                    }
                    continue
                
                # Process the instances for mitigation
                updated_instances, valid_instances = await indicator.process_existing_indicators(instances, candles)
                
                # Update the repository with the processed instances
                updated_count = 0
                for instance in updated_instances:
                    if hasattr(repository, 'update_indicator_status'):
                        success = await repository.update_indicator_status(instance)
                        if success:
                            updated_count += 1
                    elif hasattr(repository, 'update'):
                        success = await repository.update(instance.id, instance)
                        if success:
                            updated_count += 1
                
                # Record results
                results[indicator_type] = {
                    "processed": len(instances),
                    "updated": updated_count,
                    "mitigated": len(instances) - len(valid_instances),
                    "still_valid": len(valid_instances)
                }
                
                logger.info(
                    f"Processed {len(instances)} {indicator_type} instances for "
                    f"{symbol} {timeframe}: {len(valid_instances)} still valid, "
                    f"{len(instances) - len(valid_instances)} mitigated"
                )
                
            except Exception as e:
                logger.error(f"Error processing mitigation for {indicator_type}: {e}", exc_info=True)
                results[indicator_type] = {
                    "error": str(e),
                    "processed": 0,
                    "updated": 0,
                    "mitigated": 0,
                    "still_valid": 0
                }
        
        return results
    
    async def start_mitigation_service(self):
        """
        Start the mitigation service as a background task.
        This method is provided for reference, but typically the
        service would be called on-demand rather than running
        continuously in the background.
        """
        self.running = True
        logger.info("Starting mitigation service")
        
        while self.running:
            # In an actual implementation, you would:
            # 1. Query for active symbols/timeframes
            # 2. Get recent candles for each
            # 3. Call process_mitigation for each
            
            # Sleep between checks (use the smallest frequency among indicators)
            min_frequency = min(
                [freq for _, _, freq in self.registered_indicators.values()],
                default=60
            )
            
            await asyncio.sleep(min_frequency)
    
    async def stop_mitigation_service(self):
        """Stop the mitigation service."""
        self.running = False
        logger.info("Stopped mitigation service")