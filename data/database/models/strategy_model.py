from datetime import datetime, UTC
from sqlalchemy import Column, String, Integer, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship

from .base import BaseModel


class StrategyModel(BaseModel):
    """
    SQLAlchemy model for storing trading strategy information.
    Includes risk parameters and references to indicators used by the strategy.
    """
    
    __tablename__ = "strategies"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Strategy identification
    strategy_name = Column(String(100), nullable=False, unique=True)  # e.g., 'order_block_strategy', 'hidden_ob_strategy'
    strategy_type = Column(String(50), nullable=False)  # e.g., 'trend_following', 'mean_reversion', 'breakout'
    version = Column(String(20), nullable=False, default='1.0')
    
    # Description
    description = Column(String(500), nullable=True)
    author = Column(String(100), nullable=True)
    
    # Risk parameters
    max_risk_per_trade = Column(DECIMAL(5, 4), nullable=False, default=0.01)  # Maximum risk per trade (e.g., 0.01 = 1%)
    max_daily_drawdown = Column(DECIMAL(5, 4), nullable=False, default=0.05)  # Maximum daily drawdown (e.g., 0.05 = 5%)
    max_open_positions = Column(Integer, nullable=False, default=3)  # Maximum number of concurrent positions
    max_position_size = Column(DECIMAL(20, 8), nullable=True)  # Maximum position size
    
    # Trade parameters
    default_risk_reward_ratio = Column(DECIMAL(5, 2), nullable=False, default=2.0)  # Default risk:reward ratio
    default_stop_loss_percent = Column(DECIMAL(5, 4), nullable=False, default=0.02)  # Default stop loss percentage
    default_take_profit_percent = Column(DECIMAL(5, 4), nullable=False, default=0.04)  # Default take profit percentage
    use_trailing_stop = Column(Boolean, default=False, nullable=False)
    trailing_stop_percent = Column(DECIMAL(5, 4), nullable=True)
    
    # Timeframe and market configuration
    supported_timeframes = Column(ARRAY(String), nullable=False)  # List of supported timeframes
    supported_symbols = Column(ARRAY(String), nullable=True)  # List of specific symbols, or null for all
    min_market_cap = Column(DECIMAL(20, 2), nullable=True)  # Minimum market cap requirement
    min_volume = Column(DECIMAL(20, 2), nullable=True)  # Minimum volume requirement
    
    # Indicator dependencies (references to indicator registry)
    required_indicators = Column(ARRAY(Integer), nullable=True)  # Array of indicator IDs from indicators table
    primary_indicator_id = Column(Integer, ForeignKey('indicators.id'), nullable=True)  # Main indicator for signal generation
    
    # Strategy-specific parameters
    parameters = Column(JSONB, nullable=True)  # Additional strategy-specific parameters
    
    # Performance metrics (updated periodically)
    win_rate = Column(DECIMAL(5, 2), nullable=True)  # Historical win rate
    profit_factor = Column(DECIMAL(10, 2), nullable=True)  # Historical profit factor
    sharpe_ratio = Column(DECIMAL(10, 2), nullable=True)  # Sharpe ratio
    total_trades = Column(Integer, default=0, nullable=False)  # Total number of trades executed
    
    # Status and configuration
    is_active = Column(Boolean, default=True, nullable=False)  # Whether the strategy is currently active
    is_paper_trading = Column(Boolean, default=True, nullable=False)  # Whether to paper trade
    is_live_trading = Column(Boolean, default=False, nullable=False)  # Whether to live trade
    
    # Execution configuration
    execution_mode = Column(String(20), default='limit', nullable=False)  # 'market', 'limit', 'stop_limit'
    entry_order_type = Column(String(20), default='limit', nullable=False)
    exit_order_type = Column(String(20), default='limit', nullable=False)
    
    # Backtesting configuration
    backtest_start_date = Column(TIMESTAMP(timezone=True), nullable=True)
    backtest_end_date = Column(TIMESTAMP(timezone=True), nullable=True)
    backtest_initial_capital = Column(DECIMAL(20, 2), nullable=True)
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    last_executed_at = Column(TIMESTAMP(timezone=True), nullable=True)  # Last time the strategy executed
    
    # Additional metadata
    metadata_ = Column(JSONB, nullable=True)
    
    # Relationships
    primary_indicator = relationship("IndicatorModel", foreign_keys=[primary_indicator_id])
    
    # Indexes
    __table_args__ = (
        Index('idx_strategies_name', 'strategy_name'),
        Index('idx_strategies_type', 'strategy_type'),
        Index('idx_strategies_active', 'is_active'),
        Index('idx_strategies_paper', 'is_paper_trading'),
        Index('idx_strategies_live', 'is_live_trading'),
        UniqueConstraint('strategy_name', name='uq_strategy_name'),
    )
    
    def __repr__(self) -> str:
        return (
            f"Strategy({self.strategy_name}, "
            f"type={self.strategy_type}, "
            f"active={self.is_active}, "
            f"version={self.version})"
        )
    
    def to_dict(self):
        """Convert strategy model to dictionary"""
        return {
            "id": self.id,
            "strategy_name": self.strategy_name,
            "strategy_type": self.strategy_type,
            "version": self.version,
            "description": self.description,
            "author": self.author,
            "max_risk_per_trade": float(self.max_risk_per_trade) if self.max_risk_per_trade else None,
            "max_daily_drawdown": float(self.max_daily_drawdown) if self.max_daily_drawdown else None,
            "max_open_positions": self.max_open_positions,
            "max_position_size": float(self.max_position_size) if self.max_position_size else None,
            "default_risk_reward_ratio": float(self.default_risk_reward_ratio) if self.default_risk_reward_ratio else None,
            "default_stop_loss_percent": float(self.default_stop_loss_percent) if self.default_stop_loss_percent else None,
            "default_take_profit_percent": float(self.default_take_profit_percent) if self.default_take_profit_percent else None,
            "use_trailing_stop": self.use_trailing_stop,
            "trailing_stop_percent": float(self.trailing_stop_percent) if self.trailing_stop_percent else None,
            "supported_timeframes": self.supported_timeframes,
            "supported_symbols": self.supported_symbols,
            "min_market_cap": float(self.min_market_cap) if self.min_market_cap else None,
            "min_volume": float(self.min_volume) if self.min_volume else None,
            "required_indicators": self.required_indicators,
            "primary_indicator_id": self.primary_indicator_id,
            "parameters": self.parameters,
            "win_rate": float(self.win_rate) if self.win_rate else None,
            "profit_factor": float(self.profit_factor) if self.profit_factor else None,
            "sharpe_ratio": float(self.sharpe_ratio) if self.sharpe_ratio else None,
            "total_trades": self.total_trades,
            "is_active": self.is_active,
            "is_paper_trading": self.is_paper_trading,
            "is_live_trading": self.is_live_trading,
            "execution_mode": self.execution_mode,
            "entry_order_type": self.entry_order_type,
            "exit_order_type": self.exit_order_type,
            "backtest_start_date": self.backtest_start_date.isoformat() if self.backtest_start_date else None,
            "backtest_end_date": self.backtest_end_date.isoformat() if self.backtest_end_date else None,
            "backtest_initial_capital": float(self.backtest_initial_capital) if self.backtest_initial_capital else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "last_executed_at": self.last_executed_at.isoformat() if self.last_executed_at else None,
            "metadata_": self.metadata_
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create strategy model from dictionary"""
        return cls(
            id=data.get("id"),
            strategy_name=data.get("strategy_name"),
            strategy_type=data.get("strategy_type"),
            version=data.get("version", "1.0"),
            description=data.get("description"),
            author=data.get("author"),
            max_risk_per_trade=data.get("max_risk_per_trade", 0.01),
            max_daily_drawdown=data.get("max_daily_drawdown", 0.05),
            max_open_positions=data.get("max_open_positions", 3),
            max_position_size=data.get("max_position_size"),
            default_risk_reward_ratio=data.get("default_risk_reward_ratio", 2.0),
            default_stop_loss_percent=data.get("default_stop_loss_percent", 0.02),
            default_take_profit_percent=data.get("default_take_profit_percent", 0.04),
            use_trailing_stop=data.get("use_trailing_stop", False),
            trailing_stop_percent=data.get("trailing_stop_percent"),
            supported_timeframes=data.get("supported_timeframes"),
            supported_symbols=data.get("supported_symbols"),
            min_market_cap=data.get("min_market_cap"),
            min_volume=data.get("min_volume"),
            required_indicators=data.get("required_indicators"),
            primary_indicator_id=data.get("primary_indicator_id"),
            parameters=data.get("parameters", {}),
            win_rate=data.get("win_rate"),
            profit_factor=data.get("profit_factor"),
            sharpe_ratio=data.get("sharpe_ratio"),
            total_trades=data.get("total_trades", 0),
            is_active=data.get("is_active", True),
            is_paper_trading=data.get("is_paper_trading", True),
            is_live_trading=data.get("is_live_trading", False),
            execution_mode=data.get("execution_mode", "limit"),
            entry_order_type=data.get("entry_order_type", "limit"),
            exit_order_type=data.get("exit_order_type", "limit"),
            backtest_start_date=data.get("backtest_start_date"),
            backtest_end_date=data.get("backtest_end_date"),
            backtest_initial_capital=data.get("backtest_initial_capital"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            last_executed_at=data.get("last_executed_at"),
            metadata_=data.get("metadata_", {})
        )