"""
Infrastructure layer for the trading bot.

This package contains implementations for essential infrastructure components:
- Database connectivity and persistence
"""

from infrastructure.database.db import Database

__all__ = [
    'Database'
]