from dataclasses import dataclass

@dataclass
class OrderDto:
    id: str
    symbol: str
    type: str
    side: str
    price: float
    size: float
    status: str
    timestamp: str
    exchange: str = None  # Add missing fields
    signal_id: int = None
    value: float = None
    filled_size: float = None
    average_fill_price: float = None
    fee: float = None
    metadata_: dict = None
