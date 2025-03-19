from adapters.websocket.base import WebSocketClientBase
from adapters.websocket.factory import WebSocketClientFactory

class BinanceWebSocketClient(WebSocketClientBase):
    BASE_URL = "wss://stream.binance.com:9443/ws"

    def __init__(self, symbol: str, timeframe: str):
        self.symbol = symbol.lower()
        self.timeframe = timeframe

    def connect(self):
        print(f"Connecting to Binance WebSocket for {self.symbol} {self.timeframe}")

    def subscribe(self, symbol: str, timeframe: str):
        print(f"Subscribing to Binance {symbol} {timeframe}")

    def on_message(self, message: dict):
        print(f"Received message: {message}")

    def close(self):
        print("Closing Binance WebSocket")

# Register Binance client in the factory
WebSocketClientFactory.register_client("binance", BinanceWebSocketClient)
