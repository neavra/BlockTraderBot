class RestClientFactory:
    _clients = {}

    @classmethod
    def register_client(cls, exchange: str, client_class):
        cls._clients[exchange.lower()] = client_class

    @classmethod
    def get_client(cls, exchange: str):
        if exchange.lower() not in cls._clients:
            raise ValueError(f"Exchange {exchange} not supported!")
        return cls._clients[exchange.lower()]()
