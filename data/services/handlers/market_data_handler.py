"""
Handler for market data events.
"""

import logging
from typing import Any, Dict, List

from domain.events.market_events import CandleClosedEvent, CandleUpdatedEvent
#from utils.error_handling import handle_exceptions

class MarketDataHandler:
    """
    Handler for market data events.
    Processes candle events and triggers appropriate actions.
    """
    
    def __init__(self, event_bus):
        """
        Initialize the market data handler.
        
        Args:
            event_bus: Event bus for subscribing to events
        """
        self.event_bus = event_bus
        self.logger = logging.getLogger("MarketDataHandler")
        self.subscriptions = []
    
    async def initialize(self):
        """
        Initialize the handler.
        """
        self.logger.info("Initializing market data handler")
    
    async def register_handlers(self):
        """
        Register event handlers with the event bus.
        """
        # Subscribe to candle events
        self.subscriptions.append(
            self.event_bus.subscribe(CandleClosedEvent, self.handle_candle_closed)
        )
        self.subscriptions.append(
            self.event_bus.subscribe(CandleUpdatedEvent, self.handle_candle_updated)
        )
        
        self.logger.info("Registered market data event handlers")
    
    #@handle_exceptions
    async def handle_candle_closed(self, event: CandleClosedEvent) -> None:
        """
        Handle candle closed events.
        Triggered when a candle is finalized.
        
        Args:
            event: Candle closed event
        """
        candle = event.candle
        self.logger.info(
            f"Candle closed: {candle.exchange}/{candle.symbol}/{candle.timeframe} "
            f"at {candle.timestamp} - Close: {candle.close}"
        )
        
        # TODO: Add any additional logic for closed candles
        # For example:
        # - Calculate technical indicators
        # - Check trading signals
        # - Publish derived events
    
    #@handle_exceptions
    async def handle_candle_updated(self, event: CandleUpdatedEvent) -> None:
        """
        Handle candle updated events.
        Triggered when an open candle is updated.
        
        Args:
            event: Candle updated event
        """
        candle = event.candle
        self.logger.debug(
            f"Candle updated: {candle.exchange}/{candle.symbol}/{candle.timeframe} "
            f"at {candle.timestamp} - Current: {candle.close}"
        )
        
        # TODO: Add any additional logic for open candle updates
        # For example:
        # - Real-time monitoring
        # - Quick indicators calculation
    
    async def unregister_handlers(self):
        """
        Unregister all event handlers.
        """
        for subscription in self.subscriptions:
            await self.event_bus.unsubscribe(subscription)
        
        self.subscriptions.clear()
        self.logger.info("Unregistered all event handlers")