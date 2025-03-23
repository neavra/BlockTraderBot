"""
Candle consumer implementation for the trading bot.
"""

import logging
from typing import Any
import uuid

from domain.models.candle import CandleData
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
            existing_candle = await self.repository.find_by_composite_key(
                exchange=candle.exchange,
                symbol=candle.symbol,
                timeframe=candle.timeframe,
                timestamp=candle.timestamp
            )
            
            if existing_candle:
                # Update the existing candle
                self.logger.debug(f"Updating existing candle: {candle.timestamp}")
                await self.repository.update(
                    id=existing_candle.id,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume
                )
            else:
                # Create a new candle
                self.logger.debug(f"Creating new candle: {candle.timestamp}")
                await self.repository.create(candle)
            
            self.logger.debug(f"Successfully processed candle: {candle.timestamp}")
            
        except Exception as e:
            self.logger.error(f"Error processing candle: {e}")
        
    async def stop(self):
        """
        Stop the consumer.
        """
        self.logger.info("Stopping consumer")
        self.running = False