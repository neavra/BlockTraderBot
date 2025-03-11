import asyncio
import json
import websockets
import ssl
import certifi

# Binance WebSocket URL for BTC/USDT 1-minute candlestick (kline) data
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_5m"
ssl_context = ssl.create_default_context(cafile=certifi.where())

async def listen_binance_kline():
    async with websockets.connect(BINANCE_WS_URL, ssl=ssl_context) as websocket:
        while True:
            response = await websocket.recv()
            data = json.loads(response)

            # Extract relevant candlestick (kline) data
            kline = data['k']
            timestamp = data['E']  # Event time
            open_price = kline['o']
            high_price = kline['h']
            low_price = kline['l']
            close_price = kline['c']
            volume = kline['v']
            is_closed = kline['x']  # True if the candle is closed

            print(f"Timestamp: {timestamp}, Open: {open_price}, High: {high_price}, Low: {low_price}, Close: {close_price}, Volume: {volume}, Closed: {is_closed}")

# Run the WebSocket listener
asyncio.run(listen_binance_kline())
