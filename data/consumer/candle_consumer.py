"""
Candle consumer implementation for the trading bot.
"""

import asyncio
import logging
from typing import Any
import uuid

from shared.constants import Exchanges, Queues, RoutingKeys
from shared.domain.dto.candle_dto import CandleDto
from data.database.repository.candle_repository import CandleRepository
from data.database.db import Database
from shared.queue.queue_service import QueueService
from .base import BaseConsumer

class CandleConsumer(BaseConsumer[CandleDto]):
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
        self.consumer_candle_queue = QueueService()
        self.repository = None
        self.main_loop = None
        self.event_tasks = set()
        self.logger = logging.getLogger("CandleConsumer")
    
    async def initialize(self):
        """
        Initialize the consumer.
        Set up the repository for storing candles.
        """
        await super().initialize()

        self.main_loop = asyncio.get_running_loop()
        
        # Create the repository
        self.repository = CandleRepository(session=self.database.get_session())
        self.logger.info("Initialized candle consumer with repository")

        # Initialize the candle queue for consumption
        self.consumer_candle_queue.declare_exchange(Exchanges.MARKET_DATA)
        self.consumer_candle_queue.declare_queue(Queues.CANDLES)
        self.consumer_candle_queue.bind_queue(
            exchange=Exchanges.MARKET_DATA,
            queue=Queues.CANDLES,
            routing_key=RoutingKeys.CANDLE_ALL
        )
        self.consumer_candle_queue.subscribe(
            queue=Queues.CANDLES,
            callback=self.on_candle
        )
        self.logger.info("Initialised Candle Consumer Queue")
    
    def on_candle(self, candle):
        """Synchronous callback that schedules async work"""
        # Log receipt of the event synchronously
        # self.logger.info(f"Received event: {candle}")
        
        try:
            # Use run_coroutine_threadsafe to schedule the async task from this thread
            # This requires having a reference to the main event loop
            task = asyncio.run_coroutine_threadsafe(
                self.process_item(candle), 
                self.main_loop  # You need to store the main loop as an instance variable
            )

            # Track the task future
            self.event_tasks.add(task)

            def _remove_task(future):
                self.event_tasks.discard(future)
            task.add_done_callback(_remove_task)
        except Exception as e:
            self.logger.error(f"Error scheduling candle processing: {str(e)}")

    async def process_item(self, candle) -> None:
        """
        Process a candle from the queue.
        Store it in the database using the repository.
        
        Args:
            candle: Candle data to process
        """
        if not candle:
            self.logger.warning("Received empty candle data")
            return
        # self.logger.info(f"Received candle: {candle}")
        
        try:
            candleDto = CandleDto(**candle)
        
            # self.logger.info(
            #     f"Processing candle: {candleDto.exchange}/{candleDto.symbol}/{candleDto.timeframe} "
            #     f"at {candleDto.timestamp} - O:{candleDto.open} H:{candleDto.high} L:{candleDto.low} C:{candleDto.close}"
            # )
            # Check if the candle already exists:
            existing_candle = self.repository.find_by_exchange_symbol_timeframe(
                exchange=candleDto.exchange,
                symbol=candleDto.symbol,
                timeframe=candleDto.timeframe,
                timestamp=candleDto.timestamp
            )
            # self.logger.debug("Existing Candle:" + str(existing_candle))
            if len(existing_candle) > 0:
                # Update the existing candle
                # self.logger.debug(f"Updating existing candle: {candleDto.timestamp}")
                candleDto.id = existing_candle[0].id
                self.repository.update(
                    id=existing_candle[0].id,
                    domain_obj=candleDto
                )
            else:
                # Create a new candle
                # self.logger.debug(f"Creating new candle: {candleDto.timestamp}")
                self.repository.create(candleDto)
            
            # self.logger.debug(f"Successfully processed candle: {candleDto.timestamp}")
        
        except asyncio.CancelledError:
            # Handle cancellation gracefully
            self.logger.debug("Candle processing was cancelled")
            raise  # Re-raise to properly propagate cancellation

        except Exception as e:
            self.logger.error(f"CandleConsumer process_item, Error processing candle: {e}")
        
    async def stop(self):
        """
        Stop the consumer.
        """
        self.logger.info("Stopping consumer")
        self.running = False

        # Close client resources:
        self.consumer_candle_queue.stop()
        
        # Cancel all pending tasks
        tasks_to_cancel = list(self.event_tasks)
        for task in tasks_to_cancel:
            if not task.done() and not task.cancelled():
                task.cancel()
        
        # Wait for tasks with a timeout
        if tasks_to_cancel:
            try:
                # Wait up to 3 seconds for tasks to finish
                await asyncio.sleep(3)
                
                # Log any tasks that didn't complete
                for task in self.event_tasks:
                    if not task.done():
                        self.logger.warning(f"Task didn't complete during shutdown: {task}")
            except Exception as e:
                self.logger.error(f"Error waiting for candle manager tasks to complete: {e}")