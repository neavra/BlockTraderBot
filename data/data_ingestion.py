import asyncio
from managers.CandleManager import CandleManager
from database.db import DBAdapter
from database.services.candleservice import CandleService

async def start_ingestion():
    # Initialize DB session
    db_adapter = DBAdapter()

    # Initialize services with DB session
    candle_service = CandleService(db_adapter)

    # Initialize clients
    timeframes = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
    # Initialize the multi-timeframe manager
    manager = CandleManager("BTCUSDT", timeframes, candle_service)
    await manager.initialize_websockets()
    
    # Start all listeners and fetchers
    websocket_tasks = await manager.start_websocket_listeners()
    #binance_rest = BinanceRestClient()

    try:
        await asyncio.gather(*websocket_tasks)
    except Exception as e:
        print(f"Error in main process: {e}")
    finally:
        print("Closing all connections")

if __name__ == "__main__":
    asyncio.run(start_ingestion())
