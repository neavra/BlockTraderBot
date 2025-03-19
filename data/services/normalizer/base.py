from abc import ABC, abstractmethod

class NormalizerBase(ABC):
    """Base class for normalizing different exchange candle formats."""
    
    @abstractmethod
    def normalize(self, raw_candle: dict):
        """Convert exchange-specific candle data to a common format."""
        pass
