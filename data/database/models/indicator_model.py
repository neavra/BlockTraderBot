from datetime import datetime, UTC
from sqlalchemy import Column, String, Integer, TIMESTAMP, Boolean, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB

from .base import BaseModel


class IndicatorModel(BaseModel):
    """
    SQLAlchemy model for registering indicator types.
    Acts as a registry/catalog of available indicators and their configurations.
    """
    
    __tablename__ = "indicators"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Indicator identification
    indicator_name = Column(String(50), nullable=False, unique=True)  # 'order_block', 'hidden_order_block', 'fvg', 'doji', etc.
    indicator_category = Column(String(50), nullable=False)  # 'candlestick', 'structure', 'volume', 'composite', etc.
    
    # Table mapping
    table_name = Column(String(100), nullable=False)  # The actual table that stores instances of this indicator
    
    # Description and documentation
    description = Column(String(500), nullable=True)  # Description of what this indicator does
    
    # Configuration
    default_parameters = Column(JSONB, nullable=True)  # Default parameters for this indicator
    required_dependencies = Column(JSONB, nullable=True)  # List of other indicators this one depends on
    
    # Classification
    is_composite = Column(Boolean, default=False, nullable=False)  # Whether this indicator depends on other indicators
    supported_timeframes = Column(JSONB, nullable=True)  # List of timeframes this indicator supports
    
    # Status
    is_active = Column(Boolean, default=True, nullable=False)  # Whether this indicator is currently active in the system
    
    # Metadata
    version = Column(String(20), nullable=True)  # Version of the indicator implementation
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    
    # Additional configuration
    metadata_ = Column(JSONB, nullable=True)  # Additional indicator configuration metadata
    
    # Indexes and constraints
    __table_args__ = (
        # Index for lookup by name
        Index('idx_indicators_name', 'indicator_name'),
        
        # Index for category-based queries
        Index('idx_indicators_category', 'indicator_category'),
        
        # Index for active indicators
        Index('idx_indicators_active', 'is_active'),
        
        # Ensure unique indicator names
        UniqueConstraint('indicator_name', name='uq_indicator_name'),
    )
    
    def __repr__(self) -> str:
        return (
            f"Indicator({self.indicator_name}, "
            f"category={self.indicator_category}, "
            f"table={self.table_name}, "
            f"active={self.is_active})"
        )
    
    def to_dict(self):
        """Convert indicator model to dictionary"""
        return {
            "id": self.id,
            "indicator_name": self.indicator_name,
            "indicator_category": self.indicator_category,
            "table_name": self.table_name,
            "description": self.description,
            "default_parameters": self.default_parameters,
            "required_dependencies": self.required_dependencies,
            "is_composite": self.is_composite,
            "supported_timeframes": self.supported_timeframes,
            "is_active": self.is_active,
            "version": self.version,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata_": self.metadata_
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create indicator model from dictionary"""
        return cls(
            id=data.get("id"),
            indicator_name=data.get("indicator_name"),
            indicator_category=data.get("indicator_category"),
            table_name=data.get("table_name"),
            description=data.get("description"),
            default_parameters=data.get("default_parameters"),
            required_dependencies=data.get("required_dependencies"),
            is_composite=data.get("is_composite", False),
            supported_timeframes=data.get("supported_timeframes"),
            is_active=data.get("is_active", True),
            version=data.get("version"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata_=data.get("metadata_", {})
        )