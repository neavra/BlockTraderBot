from services.websocket.factory import WebSocketClientFactory
from services.rest.factory import RestClientFactory
from services.normalizer.factory import NormalizerFactory

class CandleManager:
    def __init__(self, exchange: str, symbol: str, timeframe: str):
        self.exchange = exchange.lower()
        self.symbol = symbol
        self.timeframe = timeframe

        # Get required instances dynamically from the factories
        self.websocket_client = WebSocketClientFactory.get_client(exchange, symbol, timeframe)
        self.rest_client = RestClientFactory.get_client(exchange)
        self.normalizer = NormalizerFactory.get_normalizer(exchange)

    def fetch_historical_data(self):
        """Fetch historical data using the REST client and normalize it."""
        raw_data = self.rest_client.fetch_historical_data(self.symbol, self.timeframe)
        return [self.normalizer.normalize(candle) for candle in raw_data]

    def start_realtime_feed(self):
        """Start WebSocket client to listen for real-time candles."""
        self.websocket_client.connect()
        self.websocket_client.subscribe(self.symbol, self.timeframe)

    def stop_realtime_feed(self):
        """Stop the WebSocket feed."""
        self.websocket_client.close()
