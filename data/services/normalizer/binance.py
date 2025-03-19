from services.normalizer.base import NormalizerBase
from services.normalizer.factory import NormalizerFactory

class BinanceNormalizer(NormalizerBase):
    def normalize(self, raw_candle: dict):
        return {
            "timestamp": raw_candle[0],
            "open": float(raw_candle[1]),
            "high": float(raw_candle[2]),
            "low": float(raw_candle[3]),
            "close": float(raw_candle[4]),
            "volume": float(raw_candle[5]),
        }

# Register Binance normalizer
NormalizerFactory.register_normalizer("binance", BinanceNormalizer)
