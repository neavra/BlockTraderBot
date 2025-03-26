"""
Candle consumer implementation for the trading bot.
"""

import logging
from typing import Any
import uuid

from shared.domain.models.candle import CandleData
from infrastructure.database.repository.candle_repository import CandleRepository
from infrastructure.database.db import Database
from utils.error_handling import retry #, handle_exceptions
from .base import BaseConsumer

class CandleConsumer(BaseConsumer[CandleData]):
    """
    Consumer for processing candle data from the queue.
    Stores candles in the database using the candle repository.
    """
    
    def __init__(self, database: Database):
        """
        Initialize the candle consumer.
        
        Args:
            database: Database connection
        """
        super().__init__()
        self.database = database
        self.repository = None
        self.logger = logging.getLogger("CandleConsumer")
    
    async def initialize(self):
        """
        Initialize the consumer.
        Set up the repository for storing candles.
        """
        await super().initialize()
        
        # Create the repository
        self.repository = CandleRepository(session=self.database.get_session())
        self.logger.info("Initialized candle consumer with repository")
    
    async def process_item(self, candle: CandleData) -> None:
        """
        Process a candle from the queue.
        Store it in the database using the repository.
        
        Args:
            candle: Candle data to process
        """
        if not candle:
            self.logger.warning("Received empty candle data")
            return
        
        self.logger.info(
            f"Processing candle: {candle.exchange}/{candle.symbol}/{candle.timeframe} "
            f"at {candle.timestamp} - O:{candle.open} H:{candle.high} L:{candle.low} C:{candle.close}"
        )
        
        try:
            # Check if the candle already exists TODO: Go and fix the repo function
            existing_candle = self.repository.find_by_exchange_symbol_timeframe(
                exchange=candle.exchange,
                symbol=candle.symbol,
                timeframe=candle.timeframe,
                timestamp=candle.timestamp
            )
            self.logger.debug("Existing Candle:" + str(existing_candle))
            if len(existing_candle) > 0:
                # Update the existing candle
                self.logger.debug(f"Updating existing candle: {candle.timestamp}")
                candle.id = existing_candle[0].id
                self.repository.update(
                    id=existing_candle[0].id,
                    domain_obj=candle
                )
            else:
                # Create a new candle
                self.logger.debug(f"Creating new candle: {candle.timestamp}")
                self.repository.create(candle)
            
            self.logger.debug(f"Successfully processed candle: {candle.timestamp}")
            
        except Exception as e:
            self.logger.error(f"Error processing candle: {e}")
        
    async def stop(self):
        """
        Stop the consumer.
        """
        self.logger.info("Stopping consumer")
        self.running = False