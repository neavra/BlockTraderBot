from typing import Dict, Any, Type
from strategy.indicators.base import Indicator
from strategy.strategies.base import Strategy
from strategy.domain.types.indicator_type_enum import IndicatorType

# Indicator Implementations
from strategy.indicators.composite_indicators.order_block import OrderBlockIndicator
from strategy.indicators.composite_indicators.hidden_ob import HiddenOrderBlockIndicator
from strategy.indicators.fvg import FVGIndicator
from strategy.indicators.doji_candle import DojiCandleIndicator
from strategy.indicators.bos import StructureBreakIndicator

# Repository Implementations
from data.database.repository.order_block_repository import OrderBlockRepository
# from data.database.repository.hidden_ob_repository import HiddenOBRepository
from data.database.repository.fvg_repository import FvgRepository
from data.database.repository.doji_repository import DojiRepository
from data.database.repository.bos_repository import BosRepository
from data.database.db import Database


class IndicatorFactory:
    """Factory for creating indicator instances with their repositories."""

    def __init__(self, database: Database, is_backtest: bool = False):
        """Initialize the factory with indicator and repository mappings."""
        self.is_backtest = is_backtest
        self.database = database
        session=self.database.get_session()
        
        self._indicators: Dict[IndicatorType, Type[Indicator]] = {
            IndicatorType.ORDER_BLOCK: OrderBlockIndicator,
            IndicatorType.FVG: FVGIndicator,
            IndicatorType.STRUCTURE_BREAK: StructureBreakIndicator,
            IndicatorType.DOJI_CANDLE: DojiCandleIndicator,
            # IndicatorType.HIDDEN_ORDER_BLOCK: HiddenOrderBlockIndicator,
        }

        self._indicator_repository_map: Dict[IndicatorType, Any] = {
            IndicatorType.ORDER_BLOCK: OrderBlockRepository(session=session),
            IndicatorType.FVG: FvgRepository(session=session),
            IndicatorType.STRUCTURE_BREAK: BosRepository(session=session),
            IndicatorType.DOJI_CANDLE: DojiRepository(session=session),
            # IndicatorType.HIDDEN_ORDER_BLOCK: HiddenOBRepository(session=session),
        }

    def create_indicator(self, name: IndicatorType, params: Dict[str, Any] = None) -> Indicator:
        """
        Create an indicator instance by name with its repository.

        Args:
            name: Enum value of the indicator type.
            params: Optional parameters for initialization.

        Returns:
            Instantiated indicator with its repository injected.

        Raises:
            ValueError: If the indicator type is unknown.
        """
        if name not in self._indicators:
            raise ValueError(f"Unknown indicator: {name}")

        indicator_class = self._indicators[name]
        repository = self._indicator_repository_map.get(name)

        return indicator_class(repository=repository, params=params, is_backtest=self.is_backtest)

    def register_indicator(self, name: IndicatorType, indicator_class: Type[Indicator], repository_instance: Any):
        """Register a new indicator class with its repository."""
        self._indicators[name] = indicator_class
        self._indicator_repository_map[name] = repository_instance
