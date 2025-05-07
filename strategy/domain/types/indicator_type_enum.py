from enum import Enum
from typing import Optional, Tuple

class IndicatorType(Enum):
    """
    Enum representing indicator types with their corresponding database IDs.
    This makes it easier to reference indicator records in the database.
    """
    ORDER_BLOCK = ("order_block", 1)
    FVG = ("fvg", 2)
    STRUCTURE_BREAK = ("structure_break", 3)
    DOJI_CANDLE = ("doji_candle", 4)
    HIDDEN_ORDER_BLOCK = ("hidden_order_block", 5)
    
    def __init__(self, type_name: str, indicator_id: int):
        self.type_name = type_name
        self.indicator_id = indicator_id
    
    def __str__(self):
        return self.type_name
    
    @property
    def value(self) -> str:
        """Return the string value (type name) of the enum member."""
        return self.type_name
    
    @classmethod
    def get_id_by_type(cls, indicator_type: str) -> Optional[int]:
        """
        Get the indicator ID for a given indicator type string.
        
        Args:
            indicator_type: The string value of the indicator type
            
        Returns:
            The corresponding indicator ID, or None if not found
        """
        for member in cls:
            if member.type_name == indicator_type:
                return member.indicator_id
        return None
    
    @classmethod
    def get_type_by_id(cls, indicator_id: int) -> Optional[str]:
        """
        Get the indicator type string for a given indicator ID.
        
        Args:
            indicator_id: The indicator ID
            
        Returns:
            The corresponding indicator type string, or None if not found
        """
        for member in cls:
            if member.indicator_id == indicator_id:
                return member.type_name
        return None