from enum import Enum

class IndicatorType(str, Enum):
    ORDER_BLOCK = "order_block"
    FVG = "fvg"
    STRUCTURE_BREAK = "structure_break"
    DOJI_CANDLE = "doji_candle"
    HIDDEN_ORDER_BLOCK = "hidden_order_block"
