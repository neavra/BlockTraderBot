from adapters.rest.base import RestClientBase
from adapters.rest.factory import RestClientFactory

class BinanceRestClient(RestClientBase):
    BASE_URL = "https://api.binance.com/api/v3/klines"

    def fetch_historical_data(self, symbol: str, timeframe: str, limit: int = 500):
        print(f"Fetching Binance historical data for {symbol} {timeframe}")
        return []

# Register Binance REST client
RestClientFactory.register_client("binance", BinanceRestClient)
