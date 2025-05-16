import unittest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any

from strategy.engine.mitigation_service import MitigationService
from strategy.indicators.base import Indicator
from strategy.indicators.composite_indicators.order_block import OrderBlockIndicator
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.dto.order_block_dto import OrderBlockDto
from strategy.domain.dto.fvg_dto import FvgDto
from strategy.domain.dto.doji_dto import DojiDto
from strategy.domain.dto.bos_dto import StructureBreakDto
from strategy.domain.types.indicator_type_enum import IndicatorType

class TestMitigationService(unittest.IsolatedAsyncioTestCase):
    """Test suite for MitigationService class."""
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test method."""
        # Create the mitigation service
        self.mitigation_service = MitigationService()
        
        # Create mock indicator and repository
        self.mock_repository = AsyncMock()
        self.mock_indicator = AsyncMock(spec=OrderBlockIndicator)
        self.mock_indicator.repository = self.mock_repository
        
        # Setup the mock indicator's get_relevant_price_range method
        self.mock_indicator.get_relevant_price_range.return_value = (9000.0, 11000.0)  # Mock price range
        
        # Create mock candle for DTOs
        mock_candle = CandleDto(
            symbol='BTCUSDT',
            exchange='binance',
            timeframe='1h',
            timestamp=datetime.now(timezone.utc) - timedelta(hours=3),
            open=10000.0,
            high=10500.0,
            low=9900.0,
            close=10200.0,
            volume=100.0,
            is_closed=True
        )
        
        # Mock DojiDto
        mock_doji = DojiDto(
            index=5,
            body_to_range_ratio=0.2,
            total_wick_size=500.0,
            strength=0.8,
            candle=mock_candle,
            timestamp=datetime.now(timezone.utc) - timedelta(hours=3)
        )
        
        # Mock FvgDto
        mock_fvg = FvgDto(
            type="bullish",
            top=10500.0,
            bottom=10000.0,
            size=500.0,
            size_percent=5.0,
            candle_index=4,
            filled=False,
            timestamp=datetime.now(timezone.utc) - timedelta(hours=2),
            candle=mock_candle
        )
        
        # Mock StructureBreakDto
        mock_bos = StructureBreakDto(
            index=3,
            break_type="higher_high",
            break_value=200.0,
            break_percentage=2.0,
            swing_reference=10300.0,
            candle=mock_candle,
            timestamp=datetime.now(timezone.utc) - timedelta(hours=2)
        )
        
        # Current timestamp for created_at
        now = datetime.now(timezone.utc).isoformat()
        
        # Setup mock process_existing_indicators method
        self.process_existing_indicators_result = ([
            # Mocked updated blocks (first one mitigated, second one still valid)
            OrderBlockDto(
                type='demand',
                price_high=10500.0,
                price_low=10000.0,
                index=5,
                candle=mock_candle,
                is_doji=True,
                timestamp=datetime.now(timezone.utc) - timedelta(hours=1),
                status='mitigated',
                touched=True,
                mitigation_percentage=60.0,
                timeframe='1h',
                symbol='BTCUSDT',
                exchange='binance',
                related_fvg=mock_fvg,
                doji_data=mock_doji,
                bos_data=mock_bos,
                strength=0.8,
                created_at=now
            ),
            OrderBlockDto(
                type='supply',
                price_high=11000.0,
                price_low=10800.0,
                index=3,
                candle=mock_candle,
                is_doji=True,
                timestamp=datetime.now(timezone.utc) - timedelta(hours=2),
                status='active',
                touched=True,
                mitigation_percentage=20.0,
                timeframe='1h',
                symbol='BTCUSDT',
                exchange='binance',
                related_fvg=mock_fvg,
                doji_data=mock_doji,
                bos_data=mock_bos,
                strength=0.75,
                created_at=now
            )
        ], [
            # Second element is the valid blocks list (only the second block is still valid)
            OrderBlockDto(
                type='supply',
                price_high=11000.0,
                price_low=10800.0,
                index=3,
                candle=mock_candle,
                is_doji=True,
                timestamp=datetime.now(timezone.utc) - timedelta(hours=2),
                status='active',
                touched=True,
                mitigation_percentage=20.0,
                timeframe='1h',
                symbol='BTCUSDT',
                exchange='binance',
                related_fvg=mock_fvg,
                doji_data=mock_doji,
                bos_data=mock_bos,
                strength=0.75,
                created_at=now
            )
        ])
        
        self.mock_indicator.process_existing_indicators.return_value = self.process_existing_indicators_result
        
        # Register indicator with the mitigation service
        self.mitigation_service.register_indicator(
            indicator_type=IndicatorType.ORDER_BLOCK,
            indicator=self.mock_indicator
        )
        
        # Setup mock repository methods
        self.mock_repository.find_active_indicators_in_price_range = AsyncMock(return_value=[
            # Mock existing order blocks from DB that would be returned by repository
            {
                'id': 1,
                'type': 'demand',
                'price_high': 10500.0,
                'price_low': 10000.0,
                'index': 5,
                'is_doji': True,
                'timestamp': (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                'status': 'active',
                'touched': False,
                'mitigation_percentage': 0.0,
                'timeframe': '1h',
                'symbol': 'BTCUSDT',
                'exchange': 'binance',
                'related_fvg': {
                    'type': 'bullish',
                    'top': 10500.0,
                    'bottom': 10000.0
                },
                'doji_data': {
                    'index': 5,
                    'strength': 0.8
                },
                'bos_data': {
                    'break_type': 'higher_high',
                    'break_value': 200.0
                },
                'strength': 0.8,
                'created_at': now
            },
            {
                'id': 2,
                'type': 'supply',
                'price_high': 11000.0,
                'price_low': 10800.0,
                'index': 3,
                'is_doji': True,
                'timestamp': (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                'status': 'active',
                'touched': False,
                'mitigation_percentage': 0.0,
                'timeframe': '1h',
                'symbol': 'BTCUSDT',
                'exchange': 'binance',
                'related_fvg': {
                    'type': 'bearish',
                    'top': 11000.0,
                    'bottom': 10800.0
                },
                'doji_data': {
                    'index': 3,
                    'strength': 0.75
                },
                'bos_data': {
                    'break_type': 'lower_low',
                    'break_value': 150.0
                },
                'strength': 0.75,
                'created_at': now
            }
        ])
        
        self.mock_repository.update_indicator_status = AsyncMock(return_value=True)
        
        # Create mock candles for testing
        self.mock_candles = [
            CandleDto(
                symbol='BTCUSDT',
                exchange='binance',
                timeframe='1h',
                timestamp=datetime.now(timezone.utc) - timedelta(hours=i),
                open=10000.0 + (i * 100),
                high=10500.0 + (i * 100),
                low=9900.0 + (i * 100),
                close=10200.0 + (i * 100),
                volume=100.0,
                is_closed=True
            ) for i in range(10)
        ]
    
    async def test_process_mitigation_single_indicator(self):
        """Test processing mitigation for a single registered indicator."""
        # Process mitigation with mock candles
        result = await self.mitigation_service.process_mitigation(self.mock_candles)
        
        # Verify get_relevant_price_range was called with candles
        self.mock_indicator.get_relevant_price_range.assert_called_once_with(self.mock_candles)
        
        # Verify repository method was called with correct parameters
        self.mock_repository.find_active_indicators_in_price_range.assert_called_once_with(
            exchange='binance',
            symbol='BTCUSDT',
            min_price=9000.0,
            max_price=11000.0,
            timeframes=['1h']
        )
        
        # Verify process_existing_indicators was called with instances and candles
        self.mock_indicator.process_existing_indicators.assert_called_once()
        call_args = self.mock_indicator.process_existing_indicators.call_args[0]
        self.assertEqual(len(call_args), 2)  # Should have two arguments
        self.assertEqual(len(call_args[0]), 2)  # Two order blocks in first argument
        self.assertEqual(call_args[1], self.mock_candles)  # Candles in second argument
        
        # Verify update_indicator_status was called for each updated block
        self.assertEqual(self.mock_repository.update_indicator_status.call_count, 2)
        
        # Verify correct result structure
        self.assertIn(IndicatorType.ORDER_BLOCK.value, result)
        order_block_result = result[IndicatorType.ORDER_BLOCK.value]
        self.assertEqual(order_block_result['processed'], 2)
        self.assertEqual(order_block_result['updated'], 2)
        self.assertEqual(order_block_result['mitigated'], 1)
        self.assertEqual(order_block_result['still_valid'], 1)
    
    async def test_process_mitigation_no_instances(self):
        """Test processing mitigation when no active instances are found."""
        # Set repository to return empty list
        self.mock_repository.find_active_indicators_in_price_range.return_value = []
        
        # Process mitigation with mock candles
        result = await self.mitigation_service.process_mitigation(self.mock_candles)
        
        # Verify repository method was called
        self.mock_repository.find_active_indicators_in_price_range.assert_called_once()
        
        # Verify process_existing_indicators was NOT called
        self.mock_indicator.process_existing_indicators.assert_not_called()
        
        # Verify update_indicator_status was NOT called
        self.mock_repository.update_indicator_status.assert_not_called()
        
        # Verify correct result structure for no instances
        self.assertIn(IndicatorType.ORDER_BLOCK.value, result)
        order_block_result = result[IndicatorType.ORDER_BLOCK.value]
        self.assertEqual(order_block_result['processed'], 0)
        self.assertEqual(order_block_result['updated'], 0)
        self.assertEqual(order_block_result['mitigated'], 0)
        self.assertEqual(order_block_result['still_valid'], 0)
    
    async def test_process_mitigation_error_handling(self):
        """Test error handling in process_mitigation."""
        # Set repository to raise an exception
        self.mock_repository.find_active_indicators_in_price_range.side_effect = Exception("Test error")
        
        # Process mitigation with mock candles
        result = await self.mitigation_service.process_mitigation(self.mock_candles)
        
        # Verify repository method was called
        self.mock_repository.find_active_indicators_in_price_range.assert_called_once()
        
        # Verify process_existing_indicators was NOT called
        self.mock_indicator.process_existing_indicators.assert_not_called()
        
        # Verify result contains error information
        self.assertIn(IndicatorType.ORDER_BLOCK.value, result)
        order_block_result = result[IndicatorType.ORDER_BLOCK.value]
        self.assertIn('error', order_block_result)
        self.assertEqual(order_block_result['processed'], 0)
        self.assertEqual(order_block_result['updated'], 0)
        self.assertEqual(order_block_result['mitigated'], 0)
        self.assertEqual(order_block_result['still_valid'], 0)
    
    async def test_multiple_indicators_registration(self):
        """Test registering and processing multiple indicators."""
        # Create a second mock indicator (FVG for example)
        mock_fvg_indicator = AsyncMock(spec=Indicator)
        mock_fvg_repository = AsyncMock()
        mock_fvg_indicator.repository = mock_fvg_repository
        mock_fvg_indicator.get_relevant_price_range.return_value = (9500.0, 10500.0)
        
        # Mock the results for the FVG indicator
        fvg_process_result = ([], [])  # No updated or valid FVGs for simplicity
        mock_fvg_indicator.process_existing_indicators.return_value = fvg_process_result
        
        # Set up the repository methods
        mock_fvg_repository.find_active_indicators_in_price_range = AsyncMock(return_value=[])
        
        # Create a mock IndicatorType with requires_mitigation=True
        mock_fvg_type = MagicMock()
        mock_fvg_type.value = "FVG"
        mock_fvg_type.requires_mitigation = True
        
        # Register the second indicator with the mock type that requires mitigation
        self.mitigation_service.register_indicator(
            indicator_type=mock_fvg_type,
            indicator=mock_fvg_indicator
        )
        
        # Process mitigation with mock candles
        result = await self.mitigation_service.process_mitigation(self.mock_candles)
        
        # Verify both indicators' methods were called
        self.mock_indicator.get_relevant_price_range.assert_called_once()
        mock_fvg_indicator.get_relevant_price_range.assert_called_once()
        
        self.mock_repository.find_active_indicators_in_price_range.assert_called_once()
        mock_fvg_repository.find_active_indicators_in_price_range.assert_called_once()
        
        # Verify result contains both indicators
        self.assertIn(IndicatorType.ORDER_BLOCK.value, result)
        self.assertIn("FVG", result)  # Using the value from our mock type
        
        # Check FVG results
        fvg_result = result["FVG"]
        self.assertEqual(fvg_result['processed'], 0)
        self.assertEqual(fvg_result['updated'], 0)
        self.assertEqual(fvg_result['mitigated'], 0)
        self.assertEqual(fvg_result['still_valid'], 0)

if __name__ == '__main__':
    unittest.main()