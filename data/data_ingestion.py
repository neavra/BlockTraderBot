import asyncio
from connectors.binance.BinanceClient import BinanceWebSocketClient, BinanceRestClient
from database.db import SessionLocal
from database.services.candleservice import CandleService
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

async def start_ingestion():
    # Initialize DB session
    db_session = SessionLocal()

    # Initialize services with DB session
    candle_service = CandleService(db_session)

    # Initialize clients
    binance_ws = BinanceWebSocketClient("BTCUSDT", "1m", candle_service)
    binance_rest = BinanceRestClient()

    async def fetch_historical():
        """Periodically fetch historical candlestick data via REST."""
        while True:
            try:
                data = await binance_rest.fetch_candlestick_data("BTCUSDT", "1m")
                await candle_service.store_candles(data)  # Use the service to store data
            except Exception as e:
                print(f"Error fetching historical data: {e}")
            await asyncio.sleep(300)

    # Start WebSocket listener and historical fetcher
    websocket_task = asyncio.create_task(binance_ws.listen())
    #rest_task = asyncio.create_task(fetch_historical())

    try:
        await asyncio.gather(websocket_task)
    finally:
        db_session.close()  # Ensure DB session is properly closed

if __name__ == "__main__":
    asyncio.run(start_ingestion())
