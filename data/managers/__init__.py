"""
Managers package for coordinating data flow between components.
"""
from .base import BaseManager
from .candle_manager import CandleManager

__all__ = [
    'BaseManager',
    'CandleManager',
]