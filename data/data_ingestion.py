import asyncio
from typing import List
from connectors.binance.BinanceWebClient import BinanceRestClient, BinanceWebSocketClient
from managers.CandleManager import CandleManager
from database.db import DBAdapter
from database.services.candleservice import CandleService

async def start_ingestion():
    # Initialize DB session
    db_adapter = DBAdapter()

    # Initialize services with DB session
    candle_service = CandleService(db_adapter)

    # Initialize clients
    symbols = ["BTCUSDT", "ETHUSDT"]
    #timeframes = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
    timeframes = ["1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
    exchange = "binance"
    manager_list : List[CandleManager] = []
    # Initialize the multi-timeframe managers
    for sym in symbols:
        candleMgr = CandleManager(
            candle_service=candle_service,
            rest_client=BinanceRestClient,
            websocket_client=BinanceWebSocketClient,
            symbol=sym,
            exchange=exchange,
            base_timeframes=timeframes
        )
        manager_list.append(candleMgr)

    # Start historical data population for all managers
    for manager in manager_list:
       await manager.start(lookback_days=60)
    
    # Start and collect all websocket tasks from all managers
    all_websocket_tasks = set()
    for manager in manager_list:
        websocket_tasks = await manager.process_real_time_candle()
        all_websocket_tasks.update(websocket_tasks)

    try:
        await asyncio.gather(*all_websocket_tasks)
    except KeyboardInterrupt:
        # Cancel all tasks when Ctrl+C is pressed
        for task in all_websocket_tasks:
            task.cancel()
        
        # Wait for all tasks to be cancelled
        await asyncio.gather(*all_websocket_tasks, return_exceptions=True)
        print("All websocket tasks have been cancelled")
    except Exception as e:
        print(f"Error in main process: {e}")
    finally:
        print("Closing all connections")

if __name__ == "__main__":
    asyncio.run(start_ingestion())
