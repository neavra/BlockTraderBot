from enum import Enum, auto

class SourceTypeEnum(Enum):
    """
    Enum representing different sources of candle data.
    
    This enum is used to track and differentiate between different
    sources of market data throughout the trading system.
    """
    
    # Primary data sources
    LIVE = "live"               # Real-time data from WebSocket connections
    HISTORICAL = "historical"   # Historical data from REST API calls
    
    # Additional data sources
    BACKTEST = "backtest"       # Data generated during backtesting
    
    def __str__(self) -> str:
        """Return the string value of the enum for easy serialization."""
        return self.value
    
    @classmethod
    def from_string(cls, source_str: str) -> 'SourceTypeEnum':
        """
        Convert a string representation to the corresponding enum value.
        
        Args:
            source_str: String representation of the source
            
        Returns:
            Corresponding SourceTypeEnum value
            
        Raises:
            ValueError: If the string doesn't match any enum value
        """
        for source_type in cls:
            if source_type.value == source_str:
                return source_type
        
        # Default to LIVE if not found (could also raise an error)
        raise ValueError(f"Unknown source type: {source_str}")