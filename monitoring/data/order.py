from dataclasses import dataclass

@dataclass
class Order:
    id: str
    symbol: str
    side: str
    type: str
    price: float
    size: float
    status: str
    timestamp: str
