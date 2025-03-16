import asyncio
from typing import List, Dict, Type
from managers.HistoricalCandleManager import HistoricalCandleManager
from database.services.candleservice import CandleService
from managers.CustomTFCandleManager import CustomTFCandleManager
from managers.LiveCandleManager import LiveCandleManager

"""Coordinates all candlestick operations and provides a unified interface."""
class CandleManager:
    def __init__(self, 
                 candle_service : CandleService, 
                 rest_client : Type, 
                 websocket_client: Type, 
                 symbol : str, 
                 exchange : str, 
                 base_timeframes : List[str], 
                 custom_timeframes: List[str] = None):
        # Candle db service
        self.candle_service = candle_service

        # Intialise all managers
        self.historical_manager = HistoricalCandleManager(
            candle_service=self.candle_service, 
            rest_client=rest_client, 
            symbol=symbol, 
            exchange=exchange, 
            timeframes=base_timeframes)
        
        self.live_manager = LiveCandleManager(
            symbol=symbol, 
            timeframes=base_timeframes, 
            candle_service=candle_service, 
            websocket_client_class=websocket_client
            )
        
        if custom_timeframes:
            self.timeframe_calculator = CustomTFCandleManager(
                candle_service=self.candle_service, 
                symbol=symbol, 
                exchange=exchange, 
                base_timeframes=base_timeframes, 
                custom_timeframes=custom_timeframes)
        else:
            self.timeframe_calculator = None
    
    # Main interface methods
    async def start(self, lookback_days=30): 
        # Start historical data population and maintenance
        await self.historical_manager.populate_historical_data(lookback_days)
        asyncio.create_task(self.historical_manager.run_maintenance(lookback_days=lookback_days))
       
        
        # Calculate initial custom timeframes if configured
        if self.timeframe_calculator:
            await self.timeframe_calculator.calculate_custom_timeframes()
    
    async def process_real_time_candle(self):
        # Process live candle update for standard TFs
        await self.live_manager.initialize_websockets()
        self.websocket_tasks = await self.live_manager.start_websocket_listeners()
        return self.websocket_tasks
        
    
    # Convenience methods that unify access to both base and custom timeframes
    async def get_candles(self, timeframe, limit=100):
        if timeframe in self.live_manager.timeframes:
            return self.repository.get_candles(self.live_manager.symbol, self.live_manager.exchange, timeframe, limit=limit)
        elif self.timeframe_calculator and timeframe in self.timeframe_calculator.custom_timeframes:
            return await self.timeframe_calculator.get_custom_timeframe_candles(timeframe, limit)
        else:
            raise ValueError(f"Unknown timeframe: {timeframe}")