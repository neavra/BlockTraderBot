"""
Consumer package for consuming queue data and writing to the database.
"""
from .base import BaseConsumer
from .candle_consumer import CandleConsumer

__all__ = [
    'BaseConsumer',
    'CandleConsumer',
]