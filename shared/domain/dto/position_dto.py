from dataclasses import dataclass

@dataclass
class PositionDto:
    id: str
    symbol: str
    side: str
    size: float
    entry_price: float
    current_price: float
    pnl: float
    pnl_percent: float
    timestamp: str