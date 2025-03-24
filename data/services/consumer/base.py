"""
Base consumer interface for the trading bot.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar


T = TypeVar('T')

class BaseConsumer(ABC, Generic[T]):
    """
    Abstract base class for queue consumers.
    Defines the interface for consuming messages from queues.
    """
    
    def __init__(self):
        """
        Initialize the consumer.
        
        Args:
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.running = False
    
    async def initialize(self):
        """
        Initialize the consumer.
        Override this method to perform any setup before consuming.
        """
        self.logger.info("Initializing consumer")
    
    @abstractmethod
    async def process_item(self, item: T) -> None:
        """
        Process a single item from the queue.
        Must be implemented by subclasses.
        
        Args:
            item: Item to process
        """
        pass
    
    
    async def stop(self):
        """
        Stop the consumer.
        """
        self.logger.info("Stopping consumer")
        self.running = False