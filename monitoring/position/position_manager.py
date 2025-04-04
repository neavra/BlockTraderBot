from typing import List, Dict, Any, Optional
from shared.domain.dto.position_dto import PositionDto
from data.database.db import Database
from data.database.repository.position_repository import PositionRepository
import logging 

logger = logging.getLogger(__name__)

class PositionManager:
    """Position management functionality"""
    
    def __init__(self, database: Database):
        # Create a session and initialize the repository
        session = database.get_session()
        self.repository = PositionRepository(session)
        self.logger = logger
        self.logger.info("PositionManager initialized with repository")
    
    async def get_all_positions(self) -> List[PositionDto]:
        """Get all positions from the database"""
        try:
            return self.repository.get_all()
        except Exception as e:
            self.logger.error(f"Error fetching all orders: {e}")
            return []

    # def get_position_by_symbol(self, symbol: str) -> Optional[PositionDto]:
    #     positions = [pos for pos in self.positions if pos.symbol == symbol]
    #     return positions[0] if positions else None
