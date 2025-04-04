from typing import List, Dict, Any, Optional
from data.database.repository.order_repository import OrderRepository
from data.database.db import Database
from shared.domain.dto.order_dto import OrderDto
import logging

logger = logging.getLogger(__name__)

class OrderManager:
    """Order management functionality"""
    
    def __init__(self, database: Database):
        """
        Initialize the order manager with a database connection.
        
        Args:
            database: Database connection for accessing order data
        """
        # Create a session and initialize the repository
        session = database.get_session()
        self.repository = OrderRepository(session)
        self.logger = logger
        self.logger.info("OrderManager initialized with repository")
    
    async def get_all_orders(self) -> List[OrderDto]:
        """Get all orders from the database"""
        try:
            return self.repository.get_all()
        except Exception as e:
            self.logger.error(f"Error fetching all orders: {e}")
            return []

    async def get_orders_by_symbol(self, symbol: str) -> List[OrderDto]:
        """Get orders for a specific symbol"""
        try:
            return self.repository.find_by_exchange_symbol("hyperliquid", symbol)
        except Exception as e:
            self.logger.error(f"Error fetching orders for symbol {symbol}: {e}")
            return []
            
    async def get_orders_by_status(self, status: str) -> List[OrderDto]:
        """Get orders with a specific status"""
        try:
            # Creating a custom query since there's no specific method for this
            return self.repository.find(status=status)
        except Exception as e:
            self.logger.error(f"Error fetching orders with status {status}: {e}")
            return []
    
    async def add_order(self, order: OrderDto) -> bool:
        """Add a new order to the database"""
        try:
            result = self.repository.create(order)
            return result is not None
        except Exception as e:
            self.logger.error(f"Error adding order: {e}")
            return False
    
    async def update_order(self, order_id: str, updated_data: Dict[str, Any]) -> bool:
        """Update an existing order"""
        try:
            # First get the existing order
            existing_order = self.repository.find_one(id=order_id)
            if not existing_order:
                self.logger.warning(f"Order not found for update: {order_id}")
                return False
            
            # Update fields from updated_data
            for key, value in updated_data.items():
                if hasattr(existing_order, key):
                    setattr(existing_order, key, value)
            
            # Update the order
            result = self.repository.update(existing_order.id, existing_order)
            return result is not None
        except Exception as e:
            self.logger.error(f"Error updating order {order_id}: {e}")
            return False
