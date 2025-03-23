# config/exchanges.py

EXCHANGE_CONFIG = {
    "binance": {
        "base_ws_url": "wss://stream.binance.com:9443/ws",
        "base_rest_url": "https://api.binance.com",
        "rate_limit": 1200  # Example rate limit per minute
    },
    "other_exchange": {
        "api_key": "your_other_api_key",
        "api_secret": "your_other_api_secret",
        "base_ws_url": "wss://other-exchange.com/ws",
        "base_rest_url": "https://api.other-exchange.com",
        "rate_limit": 1000
    }
}
