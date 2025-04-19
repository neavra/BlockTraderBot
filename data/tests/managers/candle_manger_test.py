import unittest
import asyncio
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch

from data.managers.candle_manager import CandleManager
from shared.domain.dto.candle_dto import CandleDto
from data.normalizer.base import Normalizer
from shared.domain.events.candle_closed_event import CandleClosedEvent


class TestCandleManager(unittest.IsolatedAsyncioTestCase):
    """Test suite for CandleManager class."""
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test method."""
        # Create mocks
        self.mock_database = MagicMock()
        self.mock_config = {
            'data': {
                'custom_timeframes': {
                    'enabled': False
                },
                'exchange': 'binance'
            }
        }
        
        # Create the manager with mocks
        self.manager = CandleManager(database=self.mock_database, config=self.mock_config)
        
        # Mock the producer queues
        self.manager.producer_candle_queue = MagicMock()
        self.manager.producer_candle_queue.publish = AsyncMock()
        self.manager.producer_event_queue = MagicMock()
        self.manager.producer_event_queue.publish = AsyncMock()
        
        # Mock the candle cache 
        self.manager.candle_cache = MagicMock()
        self.manager.candle_cache.add_to_sorted_set = AsyncMock()
        
        # Create a mock normalizer
        self.mock_normalizer = MagicMock(spec=Normalizer)
        self.mock_normalizer.normalize_rest_data = AsyncMock()
        self.mock_normalizer.to_json = MagicMock(return_value='{"mocked_json": true}')
        
        # Patch the _get_rest_normalizer method to return our mock
        patcher = patch.object(self.manager, '_get_rest_normalizer', return_value=self.mock_normalizer)
        self.addAsyncCleanup(patcher.stop)
        self.mock_get_rest_normalizer = patcher.start()
    
    async def test_handle_rest_data_empty_list(self):
        """Test handling empty REST data list."""
        result = await self.manager.handle_rest_data([], 'binance', 'BTCUSDT', '1h')
        self.assertEqual(result, [])
        self.mock_get_rest_normalizer.assert_not_called()
    
    async def test_handle_rest_data_single_candle(self):
        """Test handling a single candle from REST data."""
        # Create a mock candle that will be returned by the normalizer
        mock_candle = CandleDto(
            symbol="BTCUSDT",
            exchange="binance",
            timeframe="1h",
            timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000,
            is_closed=True
        )
        
        # Set up the normalizer to return our mock candle
        self.mock_normalizer.normalize_rest_data.return_value = mock_candle
        
        # Call the method with a single mock data item
        result = await self.manager.handle_rest_data([{'mock_data': True}], 'binance', 'BTCUSDT', '1h')
        
        # Verify results
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], mock_candle)
        
        # Verify normalizer was called correctly
        self.mock_get_rest_normalizer.assert_called_once_with('binance')
        self.mock_normalizer.normalize_rest_data.assert_called_once_with(
            data={'mock_data': True}, 
            exchange='binance', 
            symbol='BTCUSDT', 
            interval='1h'
        )
        
        # Verify publish was called (for closed candles)
        self.manager.producer_candle_queue.publish.assert_called_once()
    
    async def test_handle_rest_data_multiple_candles(self):
        """Test handling multiple candles from REST data."""
        # Create mock candles that will be returned by the normalizer
        mock_candles = [
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, i, 0, tzinfo=timezone.utc),
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                volume=1000 + i,
                is_closed=True
            ) for i in range(3)
        ]
        
        # Set up the normalizer to return our mock candles
        self.mock_normalizer.normalize_rest_data.side_effect = mock_candles
        
        # Call the method with multiple mock data items
        mock_data = [{'mock_data': i} for i in range(3)]
        result = await self.manager.handle_rest_data(mock_data, 'binance', 'BTCUSDT', '1h')
        
        # Verify results
        self.assertEqual(len(result), 3)
        self.assertEqual(result, mock_candles)
        
        # Verify normalizer was called correctly for each item
        self.assertEqual(self.mock_normalizer.normalize_rest_data.call_count, 3)
        
        # Verify publish was called for each closed candle
        self.assertEqual(self.manager.producer_candle_queue.publish.call_count, 3)
    
    async def test_handle_rest_data_error_handling(self):
        """Test error handling in handle_rest_data."""
        # Make the normalizer raise an exception
        self.mock_normalizer.normalize_rest_data.side_effect = Exception("Simulated error")
        
        # Call the method with a mock data item
        result = await self.manager.handle_rest_data([{'mock_data': True}], 'binance', 'BTCUSDT', '1h')
        
        # Verify empty result returned
        self.assertEqual(result, [])
    
    async def test_handle_rest_data_ignores_open_candles(self):
        """Test that open candles are ignored for processing."""
        # Create mock candles - one closed, one open
        closed_candle = CandleDto(
            symbol="BTCUSDT",
            exchange="binance",
            timeframe="1h",
            timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
            open=100.0,
            high=105.0,
            low=95.0,
            close=102.0,
            volume=1000,
            is_closed=True
        )
        
        open_candle = CandleDto(
            symbol="BTCUSDT",
            exchange="binance",
            timeframe="1h",
            timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
            open=102.0,
            high=105.0,
            low=98.0,
            close=103.0,
            volume=1000,
            is_closed=False
        )
        
        # Set up the normalizer to return our mock candles
        self.mock_normalizer.normalize_rest_data.side_effect = [closed_candle, open_candle]
        
        # Call the method with multiple mock data items
        mock_data = [{'mock_data': 0}, {'mock_data': 1}]
        result = await self.manager.handle_rest_data(mock_data, 'binance', 'BTCUSDT', '1h')
        
        # Verify both candles are returned
        self.assertEqual(len(result), 2)
        
        # But only the closed candle should be processed and published
        self.assertEqual(self.manager.producer_candle_queue.publish.call_count, 1)
    
    async def test_historical_complete_flag(self):
        """Test marking historical data as complete."""
        # Mark as complete
        self.manager.mark_historical_complete('binance', 'BTCUSDT', '1h')
        
        # Verify the flag is set
        key = "binance:BTCUSDT:1h"
        self.assertTrue(self.manager.historical_complete.get(key))
        
        # Check that is_any_historical_loading returns False
        # We need to mock get_configured_markets for this
        with patch.object(self.manager, 'get_configured_markets', return_value=[('binance', 'BTCUSDT', '1h')]):
            self.assertFalse(self.manager.is_any_historical_loading())
    
    # async def test_performance_1000_candles(self):
    #     """Performance test with 1000 candles."""
    #     # Create 1000 mock data items
    #     mock_data = [{'mock_data': i} for i in range(1000)]
        
    #     # Create a mock candle that will be returned by the normalizer
    #     mock_candle = CandleDto(
    #         symbol="BTCUSDT",
    #         exchange="binance",
    #         timeframe="1h",
    #         timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
    #         open=100.0,
    #         high=105.0,
    #         low=95.0,
    #         close=102.0,
    #         volume=1000,
    #         is_closed=True
    #     )
        
    #     # Set up the normalizer to always return the same candle 
    #     # (avoids creation overhead in the test)
    #     self.mock_normalizer.normalize_rest_data.return_value = mock_candle
        
    #     # Time the execution
    #     start_time = time.time()
    #     result = await self.manager.handle_rest_data(mock_data, 'binance', 'BTCUSDT', '1h')
    #     end_time = time.time()
        
    #     elapsed_time = end_time - start_time
        
    #     # Log the performance results
    #     print(f"\nPerformance Test Results:")
    #     print(f"Time to process 1000 candles: {elapsed_time:.4f} seconds")
    #     print(f"Average time per candle: {(elapsed_time/1000)*1000:.4f} ms")
        
    #     # Verify results
    #     self.assertEqual(len(result), 1000)
    #     self.assertEqual(self.mock_normalizer.normalize_rest_data.call_count, 1000)
    #     self.assertEqual(self.manager.producer_candle_queue.publish.call_count, 1000)


if __name__ == '__main__':
    unittest.main()