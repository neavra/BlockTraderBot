import unittest
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any

from strategy.engine.strategy_runner import StrategyRunner
from strategy.context.context_engine import ContextEngine
from strategy.engine.mitigation_service import MitigationService
from shared.domain.dto.signal_dto import SignalDto
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.models.market_context import MarketContext
from strategy.strategies.base import Strategy
from shared.cache.cache_service import CacheService
from shared.queue.queue_service import QueueService
from data.database.db import Database
from data.database.repository.signal_repository import SignalRepository
from shared.domain.types.source_type_enum import SourceTypeEnum


class TestStrategyRunner(unittest.IsolatedAsyncioTestCase):
    """Test suite for StrategyRunner class."""
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test method."""
        # Mock configuration dictionary
        self.config = {
            'exchange': 'binance',
            'strategy': {
                'order_block': {
                    'enabled': True,
                    'params': {
                        'lookback_period': 50,
                        'max_body_to_range_ratio': 0.4
                    }
                }
            }
        }
        
        # Mock strategies
        self.mock_strategy = MagicMock(spec=Strategy)
        self.mock_strategy.name = "TestStrategy"
        self.mock_strategy.analyze = AsyncMock(return_value=[
            SignalDto(
                id="test_signal_1",
                strategy_name="TestStrategy",
                exchange="binance",
                symbol="BTCUSDT",
                timeframe="1h",
                direction="long",
                signal_type="entry",
                price_target=50000.0,
                stop_loss=49000.0,
                execution_status="pending"
            )
        ])
        self.mock_strategy.get_requirements = MagicMock(return_value={
            'indicators': ['order_block', 'fvg'],
            'timeframes': ['1h', '4h'],
            'lookback_period': 50
        })
        self.mock_strategy.indicators = {
            'order_block': MagicMock(),
            'fvg': MagicMock()
        }
        
        # Mock cache service
        self.mock_cache = MagicMock(spec=CacheService)
        self.mock_cache.get = MagicMock(side_effect=self._mock_cache_get)
        self.mock_cache.set = MagicMock(return_value=True)
        self.mock_cache.get_from_sorted_set_by_score = MagicMock(return_value=self._get_mock_candles_json())
        
        # Mock queue services
        self.mock_producer_queue = MagicMock(spec=QueueService)
        self.mock_producer_queue.publish = AsyncMock(return_value=True)
        self.mock_producer_queue.declare_exchange = MagicMock()
        
        self.mock_consumer_queue = MagicMock(spec=QueueService)
        self.mock_consumer_queue.declare_exchange = MagicMock()
        self.mock_consumer_queue.declare_queue = MagicMock()
        self.mock_consumer_queue.bind_queue = MagicMock()
        self.mock_consumer_queue.subscribe = MagicMock()
        
        # Mock context engine
        self.mock_context_engine = MagicMock(spec=ContextEngine)
        self.mock_context_engine.update_context = AsyncMock(return_value=self._get_mock_market_context())
        self.mock_context_engine.get_multi_timeframe_contexts = AsyncMock(return_value=[self._get_mock_market_context()])
        
        # Mock database and repository
        self.mock_db = MagicMock(spec=Database)
        self.mock_db.get_session = MagicMock(return_value=MagicMock())
        self.mock_signal_repo = MagicMock(spec=SignalRepository)
        self.mock_signal_repo.bulk_create_signals = AsyncMock(return_value={"id": "test_signal_1"})
        
        # Patch the repository initialization
        with patch('data.database.repository.signal_repository.SignalRepository', return_value=self.mock_signal_repo):
            # Create StrategyRunner
            self.strategy_runner = StrategyRunner(
                strategies=[self.mock_strategy],
                cache_service=self.mock_cache,
                producer_queue=self.mock_producer_queue,
                consumer_queue=self.mock_consumer_queue,
                mitigation_service=MagicMock(spec=MitigationService),
                context_engine=self.mock_context_engine,
                database=self.mock_db,
                signal_repository=self.mock_signal_repo,
                config=self.config
            )
            
            # Mock the indicator_dag in StrategyRunner
            self.strategy_runner.indicator_dag = MagicMock()
            self.strategy_runner.indicator_dag.run_indicators = AsyncMock(return_value={
                'order_block': {'demand_blocks': [], 'supply_blocks': []},
                'fvg': {'bullish_fvgs': [], 'bearish_fvgs': []}
            })
            
            # Mock the mitigation service
            self.strategy_runner.mitigation_service.process_mitigation = AsyncMock(return_value={'order_block': {'processed': 0}})
            
            # Start the runner
            await self.strategy_runner.start()
    
    def _mock_cache_get(self, key):
        """Mock implementation of cache.get to return appropriate mock data."""
        if 'market_state' in key:
            return self._get_mock_market_context()
        elif 'candle_last_updated' in key:
            return {
                'timestamp': (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                'source': 'live'
            }
        return None
    
    def _get_mock_candles_json(self):
        """Get mock candle data in JSON format as would be returned from Redis."""
        candles = []
        base_time = datetime.now(timezone.utc)
        
        for i in range(5):
            candle_time = base_time - timedelta(hours=i)
            candle = {
                'symbol': 'BTCUSDT',
                'exchange': 'binance',
                'timeframe': '1h',
                'timestamp': candle_time.isoformat(),
                'open': 50000.0 - i * 100,
                'high': 50100.0 - i * 100,
                'low': 49900.0 - i * 100,
                'close': 50050.0 - i * 100,
                'volume': 100.0 + i * 10,
                'is_closed': True
            }
            candles.append(json.dumps(candle))
            
        return candles
    
    def _get_mock_candle_dto_list(self):
        """Create a list of CandleDto objects for testing."""
        candles = []
        base_time = datetime.now(timezone.utc)
        
        for i in range(5):
            candle_time = base_time - timedelta(hours=i)
            candle = CandleDto(
                symbol='BTCUSDT',
                exchange='binance',
                timeframe='1h',
                timestamp=candle_time,
                open=50000.0 - i * 100,
                high=50100.0 - i * 100,
                low=49900.0 - i * 100,
                close=50050.0 - i * 100,
                volume=100.0 + i * 10,
                is_closed=True
            )
            candles.append(candle)
            
        return candles
    
    def _get_mock_market_context(self):
        """Create a mock MarketContext for testing."""
        context = MarketContext(
            symbol='BTCUSDT',
            timeframe='1h',
            exchange='binance'
        )
        context.set_current_price(50050.0)
        context.set_swing_high({'price': 50200.0, 'timestamp': '2023-01-01T12:00:00Z'})
        context.set_swing_low({'price': 49800.0, 'timestamp': '2023-01-01T10:00:00Z'})
        context.set_trend('bullish')
        return context
    
    async def test_execute_on_event_full_flow(self):
        """Test the full flow from receiving an event to publishing a signal."""
        # Create a test event
        event = {
            'exchange': 'binance',
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': 'live'
        }
        
        # Create mock candle data
        mock_candles = self._get_mock_candle_dto_list()
        
        # Patch the _get_market_data_by_source method to return our mock candles
        with patch.object(self.strategy_runner, '_get_market_data_by_source', 
                        return_value=mock_candles) as mock_get_data:
            
            # Execute the method under test
            await self.strategy_runner._execute_on_event(event, SourceTypeEnum.LIVE)
            
            # Verify _get_market_data_by_source was called
            mock_get_data.assert_called_once()
            
            # Verify context engine was called correctly
            self.mock_context_engine.update_context.assert_called_once()
            self.mock_context_engine.get_multi_timeframe_contexts.assert_called_once()
            
            # Verify indicator DAG was called
            self.strategy_runner.indicator_dag.run_indicators.assert_called_once()
            
            # Verify strategy was executed
            self.mock_strategy.analyze.assert_called_once()
            
            # Verify signal was published (since source is 'live')
            self.mock_producer_queue.publish.assert_called_once()
            
            # Verify signal was saved to database
            self.mock_signal_repo.bulk_create_signals.assert_called_once()
            
            # Verify mitigation was processed
            self.strategy_runner.mitigation_service.process_mitigation.assert_called_once()
            
            # Verify cache was updated with last processed timestamp
            self.mock_cache.set.assert_called()
    
    async def test_execute_on_event_historical_data(self):
        """Test that signals are not published when processing historical data."""
        # Create a test event with historical source
        event = {
            'exchange': 'binance',
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': 'historical'
        }
        
        # Create mock candle data
        mock_candles = self._get_mock_candle_dto_list()
        
        # Create a mock signal
        mock_signal = SignalDto(
            id="test_signal_historical",
            strategy_name="TestStrategy",
            exchange="binance",
            symbol="BTCUSDT",
            timeframe="1h",
            direction="long",
            signal_type="entry",
            price_target=50000.0,
            stop_loss=49000.0,
            execution_status="pending"
        )
        
        # Set the return value for the strategy's analyze method to include our mock signal
        self.mock_strategy.analyze.return_value = [mock_signal]
        
        # Patch the _get_market_data_by_source method to return our mock candles
        with patch.object(self.strategy_runner, '_get_market_data_by_source', 
                        return_value=mock_candles) as mock_get_data:
            
            # Execute the method under test
            await self.strategy_runner._execute_on_event(event, SourceTypeEnum.HISTORICAL)
            
            # Verify _get_market_data_by_source was called
            mock_get_data.assert_called_once()
            
            # Verify context engine was called correctly
            self.mock_context_engine.update_context.assert_called_once()
            self.mock_context_engine.get_multi_timeframe_contexts.assert_called_once()
            
            # Verify indicator DAG was called
            self.strategy_runner.indicator_dag.run_indicators.assert_called_once()
            
            # Verify strategy was executed
            self.mock_strategy.analyze.assert_called_once()
            
            # Verify signal was NOT published (since source is 'historical')
            self.mock_producer_queue.publish.assert_not_called()
            
            # Verify signal was still saved to database
            self.mock_signal_repo.bulk_create_signals.assert_called_once()
            
            # Verify mitigation was processed
            self.strategy_runner.mitigation_service.process_mitigation.assert_called_once()
    
    async def test_event_callback_triggers_execution(self):
        """Test that the event callback schedules execution correctly."""
        # Create a mock event loop
        self.strategy_runner.main_loop = asyncio.get_running_loop()
        
        # Create a test event
        event = {
            'exchange': 'binance',
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Create a patch for run_coroutine_threadsafe
        with patch('asyncio.run_coroutine_threadsafe') as mock_run_coroutine:
            # Call the on_candle_event method
            self.strategy_runner._on_candle_event(event)
            
            # Verify run_coroutine_threadsafe was called
            mock_run_coroutine.assert_called_once()
    
    async def test_execute_strategies(self):
        """Test that execute_strategies calls strategies and handles signals correctly."""
        # Prepare test data
        candle_data = self._get_mock_candle_dto_list()
        market_contexts = [self._get_mock_market_context()]
        
        # Create a mock signal
        mock_signal = SignalDto(
            id="test_signal_2",
            strategy_name="TestStrategy",
            exchange="binance",
            symbol="BTCUSDT",
            timeframe="1h",
            direction="long",
            signal_type="entry",
            price_target=50000.0,
            stop_loss=49000.0,
            execution_status="pending"
        )
        
        # Set the return value for the strategy's analyze method to include our mock signal
        self.mock_strategy.analyze.return_value = [mock_signal]
        
        # Execute the method under test
        await self.strategy_runner.execute_strategies(candle_data, market_contexts, SourceTypeEnum.LIVE)
        
        # Verify the strategy was called
        self.mock_strategy.analyze.assert_called_once()
        
        # Verify signal was published
        self.mock_producer_queue.publish.assert_called_once()
        
        # Verify signal was saved to database
        self.mock_signal_repo.bulk_create_signals.assert_called_once()
    
    async def test_missing_market_data(self):
        """Test handling of missing market data."""
        # Create a test event
        event = {
            'exchange': 'binance',
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Mock cache to return None for candles
        self.mock_cache.get_from_sorted_set_by_score = MagicMock(return_value=None)
        
        # Execute the method under test
        await self.strategy_runner._execute_on_event(event, 'live')
        
        # Verify context engine was not called due to missing data
        self.mock_context_engine.update_context.assert_not_called()
        
        # Verify strategy execution was not attempted
        self.mock_strategy.analyze.assert_not_called()
    
    async def test_missing_market_context(self):
        """Test handling of missing market context."""
        # Create a test event
        event = {
            'exchange': 'binance',
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Create mock candle data
        mock_candles = self._get_mock_candle_dto_list()
        
        # Patch the _get_market_data_by_source method to return our mock candles
        with patch.object(self.strategy_runner, '_get_market_data_by_source', 
                        return_value=mock_candles) as mock_get_data:
            
            # Mock context engine to return None for multi-timeframe contexts
            self.mock_context_engine.get_multi_timeframe_contexts = AsyncMock(return_value=None)
            
            # Execute the method under test
            await self.strategy_runner._execute_on_event(event, 'live')
            
            # Verify _get_market_data_by_source was called
            mock_get_data.assert_called_once()
            
            # Verify context engine update_context was called (since we have candles)
            self.mock_context_engine.update_context.assert_called_once()
            
            # Verify get_multi_timeframe_contexts was called
            self.mock_context_engine.get_multi_timeframe_contexts.assert_called_once()
            
            # Verify strategy execution was not attempted due to missing context
            self.mock_strategy.analyze.assert_not_called()
    
    async def test_error_handling_during_execution(self):
        """Test error handling when execution fails."""
        # Create a test event
        event = {
            'exchange': 'binance',
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Create mock candle data
        mock_candles = self._get_mock_candle_dto_list()
        
        # Patch the _get_market_data_by_source method to return our mock candles
        with patch.object(self.strategy_runner, '_get_market_data_by_source', 
                        return_value=mock_candles) as mock_get_data:
            
            # Make indicator DAG raise an exception
            self.strategy_runner.indicator_dag.run_indicators = AsyncMock(side_effect=Exception("Test error"))
            
            # Execute the method under test - this should not raise an exception outside
            await self.strategy_runner._execute_on_event(event, 'live')
            
            # Verify _get_market_data_by_source was called
            mock_get_data.assert_called_once()
            
            # Verify context engine was called
            self.mock_context_engine.update_context.assert_called_once()
            self.mock_context_engine.get_multi_timeframe_contexts.assert_called_once()
            
            # Verify strategy execution was not attempted due to exception
            self.mock_strategy.analyze.assert_not_called()
            
            # Verify no signals were published
            self.mock_producer_queue.publish.assert_not_called()
    
    async def asyncTearDown(self):
        """Clean up after tests."""
        await self.strategy_runner.stop()


if __name__ == '__main__':
    unittest.main()