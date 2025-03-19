class NormalizerFactory:
    _normalizers = {}

    @classmethod
    def register_normalizer(cls, exchange: str, normalizer_class):
        cls._normalizers[exchange.lower()] = normalizer_class

    @classmethod
    def get_normalizer(cls, exchange: str):
        if exchange.lower() not in cls._normalizers:
            raise ValueError(f"Exchange {exchange} not supported!")
        return cls._normalizers[exchange.lower()]()
