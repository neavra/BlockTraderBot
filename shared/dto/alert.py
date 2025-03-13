import enum
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

class AlertType(enum.Enum):
    ORDER_PLACED = "🟢 Order Placed"
    ORDER_FILLED = "✅ Order Filled" 
    ORDER_CANCELLED = "🔴 Order Cancelled"
    ORDER_REJECTED = "❌ Order Rejected"
    POSITION_OPENED = "🟢 Position Opened"
    POSITION_CLOSED = "🔴 Position Closed"
    TAKE_PROFIT_HIT = "💰 Take Profit Hit"
    STOP_LOSS_HIT = "⚠️ Stop Loss Hit"
    ORDER_BLOCK_DETECTED = "📊 Order Block Detected"
    SIGNAL_GENERATED = "🔔 Trading Signal Generated"
    ERROR = "🚨 System Error"
    WARNING = "⚠️ System Warning"
    INFO = "ℹ️ System Info"

@dataclass
class Alert:
    type: AlertType
    symbol: str
    message: str
    timestamp: str
    details: Optional[Dict[str, Any]] = None