"""
Infrastructure layer for the trading bot.

This package contains implementations for essential infrastructure components:
- Message queues and event buses for internal communication
- Caching mechanisms for temporary data storage
- Database connectivity and persistence
"""

from infrastructure.database.db import Database

__all__ = [
    'Database'
]