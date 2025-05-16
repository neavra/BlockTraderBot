import unittest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, timezone
from typing import Dict, Any, List

# Import the strategy and related components
from strategy.strategies.order_block_strategy import OrderBlockStrategy
from strategy.domain.dto.order_block_dto import OrderBlockDto, OrderBlockResultDto
from strategy.domain.types.indicator_type_enum import IndicatorType
from strategy.domain.dto.strength_dto import StrengthDto
from shared.domain.dto.signal_dto import SignalDto
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.models.market_context import MarketContext


class TestOrderBlockStrategy(unittest.IsolatedAsyncioTestCase):
    """Test suite for OrderBlockStrategy class"""
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test method"""
        # Create params for the strategy
        self.params = {
            'risk_reward_ratio': 2.5,
            'strength_threshold': 0.7,
            'max_signals_per_day': 3,
            'stop_loss_pct': 0.02,
            'entry_buffer_pct': 0.005,
            'max_position_size': 10,
            'account_size': 1000,
            'risk_per_trade': 0.01,  # 1% of account
        }
        
        # Create mock repositories and indicators
        mock_ob_repository = AsyncMock()
        mock_ob_repository.find_active_indicators_in_price_range = AsyncMock(return_value=[])
        
        # Create mock indicators
        mock_ob_indicator = MagicMock()
        mock_ob_indicator.repository = mock_ob_repository
        
        mock_fvg_indicator = MagicMock()
        mock_fvg_indicator.repository = AsyncMock()
        
        mock_bos_indicator = MagicMock()
        mock_bos_indicator.repository = AsyncMock()
        
        mock_doji_indicator = MagicMock()
        mock_doji_indicator.repository = AsyncMock()
        
        # Create indicators dictionary with proper indicator types
        self.indicators = {
            IndicatorType.ORDER_BLOCK: mock_ob_indicator,
            IndicatorType.FVG: mock_fvg_indicator,
            IndicatorType.STRUCTURE_BREAK: mock_bos_indicator,
            IndicatorType.DOJI_CANDLE: mock_doji_indicator
        }
        
        # Create the strategy with mocked indicators
        self.strategy = OrderBlockStrategy(indicators=self.indicators, params=self.params)
        
        # Create mock market contexts for testing
        self.market_context_1h = self._create_mock_market_context("1h")
        self.market_context_4h = self._create_mock_market_context("4h") 
        self.market_contexts = [self.market_context_1h, self.market_context_4h]
        
        # Create sample order blocks for testing
        self.demand_block = OrderBlockDto(
            timeframe="1h",
            symbol="BTCUSDT",
            exchange="binance",
            type='demand',
            price_high=40000.0,
            price_low=39500.0,
            index=10,
            candle=self._create_mock_candle(),
            is_doji=True,
            timestamp=datetime.now(timezone.utc).isoformat(),
            doji_data=MagicMock(strength=0.85),
            related_fvg=MagicMock(size_percent=2.0),
            bos_data=MagicMock(break_percentage=0.015),
            status='active',
            touched=False,
            mitigation_percentage=0.0,
            created_at=datetime.now(timezone.utc).isoformat()
        )
        
        self.supply_block = OrderBlockDto(
            timeframe="1h",
            symbol="BTCUSDT",
            exchange="binance",
            type='supply',
            price_high=42000.0,
            price_low=41500.0,
            index=5,
            candle=self._create_mock_candle(),
            is_doji=True,
            timestamp=datetime.now(timezone.utc).isoformat(),
            doji_data=MagicMock(strength=0.85),
            related_fvg=MagicMock(size_percent=2.0),
            bos_data=MagicMock(break_percentage=0.015),
            status='active',
            touched=False,
            mitigation_percentage=0.0,
            created_at=datetime.now(timezone.utc).isoformat()
        )
        
        # Create data for indicator results
        self.indicator_results = {
            'order_block': OrderBlockResultDto(
                timestamp=datetime.now(timezone.utc).isoformat(),
                indicator_name='OrderBlock',
                demand_blocks=[self.demand_block],
                supply_blocks=[self.supply_block]
            ),
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'exchange': 'binance',
            'current_price': 40500.0,  # Between demand and supply blocks
            'market_contexts': self.market_contexts
        }
    
    def _create_mock_market_context(self, timeframe: str) -> MarketContext:
        """Create a mock market context for testing"""
        market_context = MarketContext(
            symbol="BTCUSDT",
            timeframe=timeframe,
            exchange="binance"
        )
        
        # Add swing points
        market_context.set_swing_high({
            'price': 41500.0,
            'index': 3,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
        market_context.set_swing_low({
            'price': 39800.0,
            'index': 8,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
        # Add Fibonacci levels
        market_context.set_fib_levels({
            'support': [
                {'price': 39600.0, 'level': 0.618, 'type': 'retracement'},
                {'price': 40000.0, 'level': 0.5, 'type': 'retracement'},
                {'price': 37500.0, 'level': 1.618, 'type': 'extension'}
            ],
            'resistance': [
                {'price': 41500.0, 'level': 0.382, 'type': 'retracement'},
                {'price': 42000.0, 'level': 0.236, 'type': 'retracement'},
                {'price': 43500.0, 'level': 1.272, 'type': 'extension'}
            ]
        })
        
        # Set current price
        market_context.set_current_price(40500.0)
        
        return market_context
    
    def _create_mock_candle(self) -> CandleDto:
        """Create a mock candle for testing"""
        return CandleDto(
            symbol="BTCUSDT",
            exchange="binance",
            timeframe="1h",
            timestamp=datetime.now(timezone.utc),
            open=40000.0,
            high=40500.0,
            low=39500.0,
            close=40200.0,
            volume=100.0,
            is_closed=True
        )
    
    async def test_analyze_method(self):
        """Test the analyze method for generating signals from order blocks"""
        # Mock the calculate_strength method to return a fixed value
        with patch.object(self.strategy, 'calculate_strength') as mock_strength:
            def strength_side_effect(block, *_):
                if block.type == 'demand':
                    return StrengthDto(
                            overall_score=0.855,
                            swing_proximity=0.9,
                            fib_confluence=0.8,
                            mtf_confluence=0.85,
                            weights={'swing_proximity': 0.4, 'fib_confluence': 0.3, 'mtf_confluence': 0.3},
                            raw_data={}
                    )
                else:
                    return StrengthDto(
                            overall_score=0.5,
                            swing_proximity=0.5,
                            fib_confluence=0.5,
                            mtf_confluence=0.5,
                            weights={'swing_proximity': 0.4, 'fib_confluence': 0.3, 'mtf_confluence': 0.3},
                            raw_data={}
                    )

            mock_strength.side_effect = strength_side_effect
            
            # Call the analyze method
            signals = await self.strategy.analyze(self.indicator_results)
            
            # Assert that we got a signal
            self.assertEqual(len(signals), 1, "Should generate one signal for the demand block")
            
            signal = signals[0]
            
            # Verify signal properties for demand block
            self.assertEqual(signal.strategy_name, "OrderBlock")
            self.assertEqual(signal.exchange, "binance")
            self.assertEqual(signal.symbol, "BTCUSDT")
            self.assertEqual(signal.timeframe, "1h")
            self.assertEqual(signal.direction, "long")
            self.assertEqual(signal.signal_type, "entry")
            self.assertEqual(signal.execution_status, "pending")
            self.assertEqual(signal.confidence_score, 0.855)
            
            # Verify price calculations
            zone_size = self.demand_block.price_high - self.demand_block.price_low
            self.assertEqual(zone_size, 500.0)
            
            # Verify entry price has appropriate buffer
            entry_buffer = self.params['entry_buffer_pct']
            expected_trigger = self.demand_block.price_low * (1 - entry_buffer)
            self.assertAlmostEqual(signal.price_target, expected_trigger, delta=1.0)
            
            # Verify stop loss has appropriate buffer
            stop_loss_pct = self.params['stop_loss_pct']
            expected_stop_loss = self.demand_block.price_low * (1 - stop_loss_pct)
            self.assertAlmostEqual(signal.stop_loss, expected_stop_loss, delta=1.0)
            
            # Verify take profit uses risk-reward ratio
            current_price = self.indicator_results['current_price']
            risk = current_price - expected_stop_loss
            expected_take_profit = current_price + (risk * self.params['risk_reward_ratio'])
            self.assertAlmostEqual(signal.take_profit, expected_take_profit, delta=1.0)
            
            # Verify risk-reward ratio is stored
            self.assertEqual(signal.risk_reward_ratio, self.params['risk_reward_ratio'])
            
            # Verify metadata
            self.assertIn('order_block_high', signal.metadata)
            self.assertIn('order_block_low', signal.metadata)
            self.assertIn('position_size', signal.metadata)
            self.assertIn('strength_details', signal.metadata)
            
            # Verify strength details in metadata
            strength_details = signal.metadata['strength_details']
            self.assertEqual(strength_details['swing_proximity'], 0.9)
            self.assertEqual(strength_details['fib_confluence'], 0.8)
            self.assertEqual(strength_details['mtf_confluence'], 0.85)
    
    async def test_analyze_with_below_threshold_strength(self):
        """Test that signals are not generated for blocks below strength threshold"""
        # Mock the calculate_strength method to return a low value
        with patch.object(self.strategy, 'calculate_strength', 
                         return_value=StrengthDto(
                             overall_score=0.5,  # Below threshold
                             swing_proximity=0.5,
                             fib_confluence=0.5,
                             mtf_confluence=0.5,
                             weights={'swing_proximity': 0.4, 'fib_confluence': 0.3, 'mtf_confluence': 0.3},
                             raw_data={}
                         )):
            
            # Call the analyze method
            signals = await self.strategy.analyze(self.indicator_results)
            
            # Assert that no signals were generated
            self.assertEqual(len(signals), 0, "Should not generate signals for blocks below strength threshold")
    
    async def test_analyze_with_price_outside_blocks(self):
        """Test that signals are generated correctly when price is outside blocks"""
        # Set price above supply block
        self.indicator_results['current_price'] = 43000.0
        
        with patch.object(self.strategy, 'calculate_strength') as mock_strength:
            def strength_side_effect(block, *_):
                if block.type == 'supply':
                    return StrengthDto(
                            overall_score=0.855,
                            swing_proximity=0.9,
                            fib_confluence=0.8,
                            mtf_confluence=0.85,
                            weights={'swing_proximity': 0.4, 'fib_confluence': 0.3, 'mtf_confluence': 0.3},
                            raw_data={}
                    )
                else:
                    return StrengthDto(
                            overall_score=0.5,
                            swing_proximity=0.5,
                            fib_confluence=0.5,
                            mtf_confluence=0.5,
                            weights={'swing_proximity': 0.4, 'fib_confluence': 0.3, 'mtf_confluence': 0.3},
                            raw_data={}
                    )

            mock_strength.side_effect = strength_side_effect
        
            
            # Call the analyze method
            signals = await self.strategy.analyze(self.indicator_results)
            
            # Assert that a signal was still generated
            self.assertEqual(len(signals), 1)
    
    async def test_calculate_strength(self):
        """Test the calculate_strength method for evaluating order blocks"""
        # Mock the score calculation methods to return fixed values
        with patch.object(self.strategy, 'calculate_swing_proximity', return_value=0.9), \
             patch.object(self.strategy, 'calculate_fib_confluence', return_value=0.8), \
             patch.object(self.strategy, 'calculate_mtf_confluence', return_value=0.7):
            
            # Calculate strength for a demand block
            strength_result = await self.strategy.calculate_strength(
                self.demand_block, self.market_contexts, [self.demand_block, self.supply_block]
            )
            
            # Verify the strength calculation formula
            expected_score = (0.4 * 0.9) + (0.3 * 0.8) + (0.3 * 0.7)  # Weighted average
            self.assertAlmostEqual(strength_result.overall_score, expected_score, places=2)
            
            # Verify component scores
            self.assertEqual(strength_result.swing_proximity, 0.9)
            self.assertEqual(strength_result.fib_confluence, 0.8)
            self.assertEqual(strength_result.mtf_confluence, 0.7)
            
            # Verify weights
            self.assertEqual(strength_result.weights['swing_proximity'], 0.4)
            self.assertEqual(strength_result.weights['fib_confluence'], 0.3)
            self.assertEqual(strength_result.weights['mtf_confluence'], 0.3)
            
            # Verify raw data
            self.assertIn('order_block_type', strength_result.raw_data)
            self.assertEqual(strength_result.raw_data['order_block_type'], 'demand')
            self.assertIn('price_range', strength_result.raw_data)
            self.assertEqual(strength_result.raw_data['price_range'], [39500.0, 40000.0])
    
    async def test_calculate_swing_proximity(self):
        """Test calculation of proximity to swing points"""
        # Test with a demand block close to a swing low
        score = self.strategy.calculate_swing_proximity(self.demand_block, self.market_contexts)
        
        # Expect high score since demand block is close to the swing low
        self.assertGreater(score, 0.65, "Should return high score for proximity to swing low")
        
        # Test with a supply block close to a swing high
        score = self.strategy.calculate_swing_proximity(self.supply_block, self.market_contexts)
        
        # Expect high score since supply block is close to the swing high
        self.assertGreater(score, 0.65, "Should return high score for proximity to swing high")
        
        # Test with a block far from swing points
        far_block = OrderBlockDto(
            timeframe="1h",
            symbol="BTCUSDT",
            exchange="binance",
            type='demand',
            price_high=38000.0,  # Far from swing low
            price_low=37000.0,
            index=10,
            candle=self._create_mock_candle(),
            is_doji=True,
            timestamp=datetime.now(timezone.utc).isoformat(),
            doji_data=MagicMock(),
            related_fvg=MagicMock(),
            bos_data=MagicMock(),
            status='active',
            touched=False,
            mitigation_percentage=0.0,
            created_at=datetime.now(timezone.utc).isoformat()
        )
        
        score = self.strategy.calculate_swing_proximity(far_block, self.market_contexts)
        
        # Expect lower score for block far from swing points
        self.assertLess(score, 0.7, "Should return lower score for block far from swing points")
    
    async def test_calculate_fib_confluence(self):
        """Test calculation of Fibonacci level confluence"""
        # Test with a demand block close to a Fibonacci support level
        score = self.strategy.calculate_fib_confluence(self.demand_block, self.market_contexts)
        
        # Expect high score since demand block is close to the 0.618 retracement level
        self.assertGreater(score, 0.65, "Should return high score for confluence with Fibonacci level")
        
        # Test with a supply block close to a Fibonacci resistance level
        score = self.strategy.calculate_fib_confluence(self.supply_block, self.market_contexts)
        
        # Expect high score since supply block is close to the 0.236 retracement level
        self.assertGreater(score, 0.65, "Should return high score for confluence with Fibonacci level")
        
        # Test with a block far from Fibonacci levels
        far_block = OrderBlockDto(
            timeframe="1h",
            symbol="BTCUSDT",
            exchange="binance",
            type='demand',
            price_high=45000.0,  # Far from any Fibonacci level
            price_low=44500.0,
            index=10,
            candle=self._create_mock_candle(),
            is_doji=True,
            timestamp=datetime.now(timezone.utc).isoformat(),
            doji_data=MagicMock(),
            related_fvg=MagicMock(),
            bos_data=MagicMock(),
            status='active',
            touched=False,
            mitigation_percentage=0.0,
            created_at=datetime.now(timezone.utc).isoformat()
        )
        
        score = self.strategy.calculate_fib_confluence(far_block, self.market_contexts)
        
        # Expect lower score for block far from Fibonacci levels
        self.assertLess(score, 0.7, "Should return lower score for block far from Fibonacci levels")
    
    async def test_validate_signal(self):
        """Test signal validation logic"""
        # Create a valid signal
        valid_signal = SignalDto(
            strategy_name="OrderBlock",
            exchange="binance",
            symbol="BTCUSDT",
            timeframe="1h",
            direction="long",
            signal_type="entry",
            price_target=39300.0,
            stop_loss=38600.0,
            take_profit=41000.0,
            risk_reward_ratio=2.5,
            confidence_score=0.85,
            execution_status="pending"
        )
        
        # Test validation of a valid signal
        result = self.strategy.validate_signal(valid_signal)
        self.assertTrue(result, "Should validate a valid signal")
        
        # Test with missing required fields
        invalid_signal = SignalDto(
            strategy_name="OrderBlock",
            symbol="",  # Missing symbol
            exchange="binance",
            timeframe="1h",
            direction="long",
            signal_type="entry",
            price_target=39300.0,
            stop_loss=38600.0,
            take_profit=41000.0,
            confidence_score=0.85
        )
        
        result = self.strategy.validate_signal(invalid_signal)
        self.assertFalse(result, "Should invalidate signal with missing required fields")
        
        # Test with invalid direction
        invalid_direction_signal = SignalDto(
            strategy_name="OrderBlock",
            symbol="BTCUSDT",
            exchange="binance",
            timeframe="1h",
            direction="sideways",  # Invalid direction
            signal_type="entry",
            price_target=39300.0,
            stop_loss=38600.0,
            take_profit=41000.0,
            confidence_score=0.85
        )
        
        result = self.strategy.validate_signal(invalid_direction_signal)
        self.assertFalse(result, "Should invalidate signal with invalid direction")
        
        # Test with low risk-reward ratio
        low_rr_signal = SignalDto(
            strategy_name="OrderBlock",
            symbol="BTCUSDT",
            exchange="binance",
            timeframe="1h",
            direction="long",
            signal_type="entry",
            price_target=39300.0,
            stop_loss=39000.0,
            take_profit=39600.0,  # Low risk-reward
            confidence_score=0.85
        )
        
        # Should calculate R:R internally
        result = self.strategy.validate_signal(low_rr_signal)
        self.assertFalse(result, "Should invalidate signal with low risk-reward ratio")
    
    async def test_calculate_position_size(self):
        """Test position size calculation based on risk management"""
        # Calculate position size for a long trade
        entry_price = 40000.0
        stop_loss = 39000.0
        risk_percentage = 0.01  # 1% of account
        
        position_size = self.strategy._calculate_position_size(
            entry_price, stop_loss, risk_percentage
        )
        
        # Expected calculation:
        # Risk amount = account_size * risk_percentage = 1000 * 0.01 = 10
        # Price risk = |entry_price - stop_loss| = |40000 - 39000| = 1000
        # Position size = risk_amount / price_risk = 10 / 1000 = 0.01
        expected_position_size = 0.01
        
        self.assertAlmostEqual(position_size, expected_position_size, delta=0.001)
        
        # Test with max position size limit
        # Set a small max position size
        self.strategy.params['max_position_size'] = 0.005
        
        position_size = self.strategy._calculate_position_size(
            entry_price, stop_loss, risk_percentage
        )
        
        # Position size should be capped at max_position_size
        self.assertEqual(position_size, 0.005)


if __name__ == '__main__':
    unittest.main()