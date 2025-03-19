

class WebSocketClientFactory:
    """Factory to register and create WebSocket clients dynamically."""
    
    _clients = {}

    @classmethod
    def register_client(cls, exchange: str, client_class):
        """Register a WebSocket client for a specific exchange."""
        cls._clients[exchange.lower()] = client_class

    @classmethod
    def get_client(cls, exchange: str, symbol: str, timeframe: str):
        """Retrieve a WebSocket client without modifying this factory."""
        if exchange.lower() not in cls._clients:
            raise ValueError(f"Exchange {exchange} not supported!")
        return cls._clients[exchange.lower()](symbol, timeframe)
