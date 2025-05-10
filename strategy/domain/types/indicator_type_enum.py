from enum import Enum
from typing import Optional, Tuple

class IndicatorType(Enum):
    """
    Enum representing indicator types with their corresponding database IDs.
    This makes it easier to reference indicator records in the database.
    The requires_mitigation field indicates whether this indicator type
    needs to be processed for mitigation.
    """
    ORDER_BLOCK = ("order_block", 1, True)
    FVG = ("fvg", 2, False) # Ignore mitigation for FVGs for now
    STRUCTURE_BREAK = ("structure_break", 3, False)
    DOJI_CANDLE = ("doji_candle", 4, False)
    HIDDEN_ORDER_BLOCK = ("hidden_order_block", 5, True)
    
    def __init__(self, type_name: str, indicator_id: int, requires_mitigation: bool):
        self.type_name = type_name
        self.indicator_id = indicator_id
        self.requires_mitigation = requires_mitigation
    
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
    
    @classmethod
    def get_mitigated_types(cls) -> list['IndicatorType']:
        """
        Get a list of indicator types that require mitigation.
        
        Returns:
            List of IndicatorType enum members that need mitigation
        """
        return [member for member in cls if member.requires_mitigation]
    
    @classmethod
    def get_by_type_name(cls, type_name: str) -> Optional['IndicatorType']:
        """
        Get the enum member for a given type name.
        
        Args:
            type_name: The string type name (e.g., 'order_block', 'fvg')
            
        Returns:
            The corresponding enum member, or None if not found
        """
        for member in cls:
            if member.type_name == type_name:
                return member
        return None