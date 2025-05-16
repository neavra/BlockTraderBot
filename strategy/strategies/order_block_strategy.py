from typing import Dict, Any, Optional, List
from strategy.strategies.base import Strategy
from strategy.indicators.base import Indicator
from shared.domain.dto.signal_dto import SignalDto
from strategy.domain.models.market_context import MarketContext
from strategy.domain.types.indicator_type_enum import IndicatorType
from strategy.domain.dto.strength_dto import StrengthDto
from strategy.domain.dto.order_block_dto import OrderBlockDto, OrderBlockResultDto
from strategy.domain.types.time_frame_enum import TIMEFRAME_HIERARCHY
from data.database.repository.order_block_repository import OrderBlockRepository

import logging


logger = logging.getLogger(__name__)

class OrderBlockStrategy(Strategy):
    """
    Strategy that looks for Order Blocks
    Order blocks are areas of significant market imbalance where institutional orders are executed,
    often marked by a strong price reversal.
    """
    
    def __init__(self, indicators: Dict[str, Indicator] = None, params: Dict[str, Any] = None):
        """
        Initialize the Order Block strategy
        
        Args:
            indicators: Dictionary of indicators (will create defaults if None)
            params: Strategy parameters
        """
        default_params = {
            'risk_reward_ratio': 2.0,
            'strength_threshold': 0.7,
            'max_signals_per_day': 3,
            'stop_loss_pct': 0.02,
            'entry_buffer_pct': 0.005,
            'max_position_size': 10,
            'account_size': 1000,
        }
        
        if params:
            default_params.update(params)
        
        # Validate required indicators
        if indicators is None:
            raise ValueError("Indicators must be provided for Order Block strategy")
        
        required_indicators = [IndicatorType.ORDER_BLOCK, IndicatorType.FVG, IndicatorType.STRUCTURE_BREAK, IndicatorType.DOJI_CANDLE]
        for indicator_name in required_indicators:
            if indicator_name not in indicators:
                raise ValueError(f"Missing required indicator: {indicator_name}")
        
        self.order_block_repository = indicators.get(IndicatorType.ORDER_BLOCK).repository
        if not self.order_block_repository:
            raise ValueError(f"Unable to get repository from Order Block Indicator")
        
        super().__init__("OrderBlock", indicators, default_params)

    async def analyze(self, data: Dict[str, Any]) -> Optional[List[SignalDto]]:
        """
        Analyze indicators and generate trading signals.
        
        Args:
            data: Dictionary with indicator results
            
        Returns:
            List of SignalDto objects
        """
        signals = []
        
        # Get order block results
        order_block_results: OrderBlockResultDto = data.get('order_block', {})
        if not order_block_results:
            return []
        
        # Extract relevant data
        demand_blocks = order_block_results.demand_blocks
        supply_blocks = order_block_results.supply_blocks
        market_contexts: List[MarketContext] = data.get('market_contexts', [])
        
        if not market_contexts:
            return []
        
        # Get current price
        current_price = data.get('current_price')
        if not current_price:
            return []
        
        # Process demand (bullish) order blocks
        for block in demand_blocks:
            # Skip if not active or status is not 'active'
            if block.status != 'active':
                continue
            
            # Calculate strength score
            results: StrengthDto = await self.calculate_strength(block, market_contexts, demand_blocks + supply_blocks)
            block.strength = results.overall_score
            swing_score = results.swing_proximity
            fib_score = results.fib_confluence
            mtf_score = results.mtf_confluence
            
            # Check if strength exceeds threshold
            if block.strength < self.params['strength_threshold']:
                continue
            
            entry_buffer = self.params.get('entry_buffer_pct', 0.005)
            trigger_price = block.price_low * (1 - entry_buffer)
            stop_loss_pct = self.params.get('stop_loss_pct', 0.02)
            stop_loss = block.price_low * (1 - stop_loss_pct)
            risk = current_price - stop_loss
            take_profit = current_price + (risk * self.params['risk_reward_ratio'])
            
            # Calculate position size based on risk management
            position_size = self._calculate_position_size(
                current_price, stop_loss, self.params['risk_per_trade']
            )
            
            # Create signal
            signal = SignalDto(
                strategy_name=self.name,
                exchange=data.get('exchange'),
                symbol=data.get('symbol'),
                timeframe=data.get('timeframe'),
                direction='long',
                signal_type='entry',
                price_target=trigger_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                risk_reward_ratio=self.params['risk_reward_ratio'],
                confidence_score=block.strength,
                execution_status='pending',
                metadata={
                    'order_block_high': block.price_high,
                    'order_block_low': block.price_low,
                    'position_size': position_size,
                    'strength_details': {
                        'swing_proximity': swing_score,
                        'fib_confluence': fib_score,
                        'mtf_confluence': mtf_score
                    }
                }
            )

            # Validate signal
            if self.validate_signal(signal):
                signals.append(signal)

        # Process supply (bearish) order blocks
        for block in supply_blocks:
            if block.status != 'active':
                continue
            
            results: StrengthDto = await self.calculate_strength(block, market_contexts, demand_blocks + supply_blocks)
            block.strength = results.overall_score
            swing_score = results.swing_proximity
            fib_score = results.fib_confluence
            mtf_score = results.mtf_confluence
            
            if block.strength < self.params['strength_threshold']:
                continue
            
            entry_buffer = self.params.get('entry_buffer_pct', 0.005)
            trigger_price = block.price_low * (1 + entry_buffer)
            stop_loss_pct = self.params.get('stop_loss_pct', 0.02)
            stop_loss = block.price_high * (1 + stop_loss_pct)
            risk = stop_loss - current_price
            take_profit = current_price - (risk * self.params['risk_reward_ratio'])

            position_size = self._calculate_position_size(
                current_price, stop_loss, self.params['risk_per_trade']
            )

            signal = SignalDto(
                strategy_name=self.name,
                exchange=data.get('exchange'),
                symbol=data.get('symbol'),
                timeframe=data.get('timeframe'),
                direction='short',
                signal_type='entry',
                price_target=trigger_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                risk_reward_ratio=self.params['risk_reward_ratio'],
                confidence_score=block.strength,
                execution_status='pending',
                metadata={
                    'order_block_high': block.price_high,
                    'order_block_low': block.price_low,
                    'position_size': position_size,
                    'strength_details': {
                        'swing_proximity': swing_score,
                        'fib_confluence': fib_score,
                        'mtf_confluence': mtf_score
                    }
                }
            )

            if self.validate_signal(signal):
                signals.append(signal)
        
        return signals


    def _calculate_position_size(self, entry_price, stop_loss, risk_percentage):
        """Calculate position size based on risk management rules"""
        # TODO, finish logic for account size tracking
        account_size = self.params['account_size']
        risk_amount = account_size * risk_percentage
        
        # Calculate position size
        price_risk = abs(entry_price - stop_loss)
        position_size = risk_amount / price_risk
        
        # Apply position size limits
        max_position_size = self.params['max_position_size']
        if max_position_size and position_size > max_position_size:
            position_size = max_position_size
        
        return position_size

    def validate_signal(self, signal: SignalDto) -> bool:
        """
        Validate if a signal should be executed based on structural integrity,
        risk parameters, and existing market conditions. Take note actual check on whether this 
        signal is worth trading is done at the execution level.
        
        Args:
            signal: The signal to validate
            market_context: Current market context information
            
        Returns:
            True if the signal is valid and should be executed, False otherwise
        """
        # 1. Data structure validation - ensure required fields exist
        if not signal.symbol or not signal.exchange or not signal.timeframe:
            logger.warning(f"Signal missing required fields: {signal}")
            return False
        
        if signal.direction not in ['long', 'short']:
            logger.warning(f"Invalid signal direction: {signal.direction}")
            return False
        
        # 2. Price targets validation
        if signal.price_target is None:
            logger.warning("Signal missing entry price target")
            return False
        
        if signal.stop_loss is None:
            logger.warning("Signal missing stop loss price")
            return False
        
        if signal.take_profit is None:
            logger.warning("Signal missing take profit price")
            return False

        # 3. Logical Price check
        min_rr = self.params.get('min_risk_reward_ratio', 1.5)
        if not signal.risk_reward_ratio:
            # Calculate R:R if not provided
            if signal.direction == 'long':
                risk = abs(signal.price_target - signal.stop_loss)
                reward = abs(signal.take_profit - signal.price_target)
            else:  # short
                risk = abs(signal.stop_loss - signal.price_target)
                reward = abs(signal.price_target - signal.take_profit)
            
            if risk == 0:
                logger.warning("Invalid signal: Risk is zero")
                return False
                
            calculated_rr = reward / risk
            signal.risk_reward_ratio = calculated_rr
        
        if signal.risk_reward_ratio < min_rr:
            logger.info(f"Signal R:R ratio {signal.risk_reward_ratio:.2f} below minimum {min_rr}")
            return False

        return True
    
    async def calculate_strength(self, order_block: OrderBlockDto, market_contexts: List[MarketContext], all_order_blocks: List[OrderBlockDto]) -> StrengthDto:
        """
        Calculate the strength score of an order block based on multiple factors.
        
        Args:
            order_block: The order block to evaluate
            market_context: Market context containing swing points and fib levels
            all_order_blocks: List of all order blocks for MTF analysis
            
        Returns:
            StrengthDto containing the overall strength score and detailed component scores
        """
        # Weights (adjust based on your testing)
        weights = {
            'swing_proximity': 0.4,
            'fib_confluence': 0.3,
            'mtf_confluence': 0.3
        }
        
        # Calculate individual scores
        swing_score = self.calculate_swing_proximity(order_block, market_contexts)
        fib_score = self.calculate_fib_confluence(order_block, market_contexts)
        mtf_score = await self.calculate_mtf_confluence(order_block, all_order_blocks)
        
        # Weighted sum for overall strength
        overall_score = (
            weights['swing_proximity'] * swing_score +
            weights['fib_confluence'] * fib_score +
            weights['mtf_confluence'] * mtf_score
        )
        
        # Create strength DTO
        return StrengthDto(
            overall_score=overall_score,
            swing_proximity=swing_score,
            fib_confluence=fib_score,
            mtf_confluence=mtf_score,
            weights=weights,
            raw_data={
                'order_block_id': getattr(order_block, 'id', None),
                'order_block_type': getattr(order_block, 'type', None),
                'price_range': [getattr(order_block, 'price_low', 0), getattr(order_block, 'price_high', 0)]
            }
        )

    def calculate_swing_proximity(self, order_block: OrderBlockDto, market_contexts: List[MarketContext]):
        """
        Calculate proximity to swing points across multiple timeframes.
        
        Args:
            order_block: The order block to evaluate
            market_contexts: List of market contexts ordered by timeframe
            
        Returns:
            Score between 0-1 indicating proximity to significant swing points
        """
        # If no market contexts are available, return 0
        if not market_contexts:
            return 0.0
        
        # Extract order block price range and timeframe
        ob_high = order_block.price_high
        ob_low = order_block.price_low
        ob_type = order_block.type  # 'demand' or 'supply'
        ob_timeframe = order_block.timeframe
        
        # Get the relevant hierarchy for this order block
        timeframe_list = TIMEFRAME_HIERARCHY.get(ob_timeframe, [ob_timeframe])
        
        # Initialize scores
        proximity_scores = []
        
        # Iterate through market contexts, checking swing points in each
        for i, context in enumerate(market_contexts):
            # Skip if context doesn't have swing points
            if not context.swing_high or not context.swing_low:
                continue
            
            # Get swing values from market context
            swing_high = context.swing_high.get('price') if isinstance(context.swing_high, dict) else getattr(context.swing_high, 'price', None)
            swing_low = context.swing_low.get('price') if isinstance(context.swing_low, dict) else getattr(context.swing_low, 'price', None)
            context_timeframe = context.timeframe
            
            # Skip if swing points aren't valid
            if swing_high is None or swing_low is None:
                continue
            
            # Step 1: Calculate proximity score based on block type
            if ob_type == 'demand':
                # For demand blocks, check proximity to swing low
                if ob_low <= swing_low <= ob_high:
                    # Direct hit - swing low is inside the order block
                    proximity = 1.0
                else:
                    # Calculate distance from order block to swing low
                    distance = min(abs(ob_low - swing_low), abs(ob_high - swing_low))
                    relative_distance = distance / swing_low  # Normalize by price level
                    
                    # Convert to proximity score (0-1)
                    # Within 2% is considered close (0.8+), beyond 5% is distant
                    proximity = max(0, 1 - (relative_distance / 0.05))
            else:  # supply block
                # For supply blocks, check proximity to swing high
                if ob_low <= swing_high <= ob_high:
                    # Direct hit - swing high is inside the order block
                    proximity = 1.0
                else:
                    # Calculate distance from order block to swing high
                    distance = min(abs(ob_low - swing_high), abs(ob_high - swing_high))
                    relative_distance = distance / swing_high  # Normalize by price level
                    
                    # Convert to proximity score (0-1)
                    proximity = max(0, 1 - (relative_distance / 0.05))
            
            # Step 2: Apply timeframe weighting
            # Calculate position in the hierarchy (higher = better)
            weighted_score = 0
            if proximity > 0:
                # Find the position of the context timeframe in the hierarchy
                if context_timeframe in timeframe_list:
                    tf_index = timeframe_list.index(context_timeframe)
                    # Normalize to 0-1 range, with higher timeframes getting higher weights
                    tf_position = tf_index / max(1, len(timeframe_list) - 1)  # 0 for lowest, 1 for highest
                    
                    # Apply stronger emphasis on higher timeframes
                    # Use exponential scaling to emphasize higher timeframes more
                    tf_weight = 0.6 + (0.4 * tf_position ** 2)  # Scale from 0.6 to 1.0 with exponential increase
                else:
                    # Default weight for timeframes not in our expected hierarchy
                    tf_weight = 0.5
                
                # Combine proximity and timeframe weight
                # The final score is weighted more toward higher timeframes
                weighted_score = proximity * tf_weight
                
                # Add to our list of scores
                proximity_scores.append(weighted_score)
        
        # Step 3: Normalize the final score
        if proximity_scores:
            # Take the maximum score across all timeframes
            final_score = max(proximity_scores)
        else:
            final_score = 0.0
        
        return final_score
    
    def calculate_fib_confluence(self, order_block: OrderBlockDto, market_contexts: List[MarketContext]):
        """
        Calculate confluence with Fibonacci levels across multiple timeframes.
        
        Args:
            order_block: The order block to evaluate
            market_contexts: List of market contexts ordered by timeframe
            
        Returns:
            Score between 0-1 indicating confluence with significant Fibonacci levels
        """
        # If no market contexts are available, return 0
        if not market_contexts:
            return 0.0
        
        # Extract order block price range and timeframe
        ob_high = order_block.price_high
        ob_low = order_block.price_low
        ob_type = order_block.type  # 'demand' or 'supply'
        ob_timeframe = order_block.timeframe
        
        # Get the relevant hierarchy for this order block
        timeframe_list = TIMEFRAME_HIERARCHY.get(ob_timeframe, [ob_timeframe])
        
        # Initialize scores
        confluence_scores = []
        
        # Iterate through market contexts, checking fib levels in each
        for i, context in enumerate(market_contexts):
            # Skip if context doesn't have Fibonacci levels
            if not context.fib_levels:
                continue
            
            context_timeframe = context.timeframe
            
            # Get relevant Fibonacci levels based on order block type
            fib_levels = []
            if ob_type == 'demand':
                fib_levels = context.fib_levels.get('support', [])
            else:  # supply block
                fib_levels = context.fib_levels.get('resistance', [])
            
            # Skip if no relevant Fibonacci levels
            if not fib_levels:
                continue
            
            # Check if the order block overlaps with any Fibonacci level
            max_level_confluence = 0.0
            
            for level in fib_levels:
                level_price = level.get('price')
                if level_price is None:
                    continue
                    
                # Calculate confluence score
                if ob_low <= level_price <= ob_high:
                    # Direct hit - Fibonacci level is inside the order block
                    # The more important the level, the higher the score
                    level_type = level.get('type', '')
                    level_value = level.get('level', 0.5)
                    
                    # Higher weight for key levels (0.618, 0.5, 0.382)
                    level_weight = 1.0
                    if level_type == 'retracement':
                        if abs(level_value - 0.618) < 0.001:
                            level_weight = 1.0  # Golden ratio
                        elif abs(level_value - 0.5) < 0.001:
                            level_weight = 0.95  # Midpoint
                        elif abs(level_value - 0.382) < 0.001:
                            level_weight = 0.9  # Also important

                    elif level_type == 'extension':
                        # Key extension levels
                        if abs(level_value - 1.618) < 0.001:
                            level_weight = 1.0  # Golden ratio extension
                        elif abs(level_value - 1.272) < 0.001:
                            level_weight = 0.95  # Square root of 1.618
                        elif abs(level_value - 2.0) < 0.001:
                            level_weight = 0.9  # 2x extension
                        elif abs(level_value - 2.618) < 0.001:
                            level_weight = 0.85  # 1.618 x 1.618
                    
                    # Perfect confluence
                    level_confluence = 1.0 * level_weight
                else:
                    # Calculate distance from order block to Fibonacci level
                    distance = min(abs(ob_low - level_price), abs(ob_high - level_price))
                    avg_price = (ob_high + ob_low) / 2
                    relative_distance = distance / avg_price
                    
                    # Convert to confluence score (0-1)
                    # Within 1% is considered close (0.8+), beyond 3% is distant
                    level_confluence = max(0, 1 - (relative_distance / 0.03))
                
                # Keep the highest confluence score among all levels
                max_level_confluence = max(max_level_confluence, level_confluence)
            
            # If we found confluence with any level
            if max_level_confluence > 0:
                # Apply timeframe weighting
                # Find the position of the context timeframe in the hierarchy
                if context_timeframe in timeframe_list:
                    tf_index = timeframe_list.index(context_timeframe)
                    # Normalize to 0-1 range, with higher timeframes getting higher weights
                    tf_position = tf_index / max(1, len(timeframe_list) - 1)  # 0 for lowest, 1 for highest
                    
                    # Apply stronger emphasis on higher timeframes
                    # Use exponential scaling to emphasize higher timeframes more
                    tf_weight = 0.6 + (0.4 * tf_position ** 2)  # Scale from 0.6 to 1.0 with exponential increase
                else:
                    # Default weight for timeframes not in our expected hierarchy
                    tf_weight = 0.5
                
                # Combine confluence and timeframe weight
                weighted_score = max_level_confluence * tf_weight
                
                # Add to our list of scores
                confluence_scores.append(weighted_score)
        
        # Normalize the final score
        if confluence_scores:
            # Take the maximum score across all timeframes
            # This prioritizes strong confluence in higher timeframes
            final_score = max(confluence_scores)
        else:
            final_score = 0.0
        
        return final_score
    
    async def calculate_mtf_confluence(self, order_block: OrderBlockDto, local_order_blocks: List[OrderBlockDto]):
        """
        Calculate multi-timeframe confluence score for an order block by finding
        overlapping order blocks from higher timeframes in the database.
        
        Args:
            order_block: The order block to evaluate
            local_order_blocks: Local set of order blocks from current analysis
            
        Returns:
            Confluence score between 0.0 and 1.0
        """
        # Get current order block's price range
        ob_high = order_block.price_high
        ob_low = order_block.price_low
        ob_timeframe = order_block.timeframe
        ob_symbol = order_block.symbol
        ob_exchange = order_block.exchange
        
        # Get list of higher timeframes to check from TIMEFRAME_HIERARCHY
        higher_timeframes = TIMEFRAME_HIERARCHY.get(ob_timeframe, [])
        if not higher_timeframes:
            return 0.0  # No higher timeframes to check
        
        try:
            # Find min/max price range across all local order blocks to expand search range
            all_highs = [ob.price_high for ob in local_order_blocks]
            all_lows = [ob.price_low for ob in local_order_blocks]
            
            # Add current order block's range
            all_highs.append(ob_high)
            all_lows.append(ob_low)
            
            # Calculate expanded price range with a buffer (10%)
            min_price = min(all_lows) * 0.9
            max_price = max(all_highs) * 1.1
            
            # Query repository for active order blocks in higher timeframes within price range
            mtf_order_blocks = await self.order_block_repository.find_active_indicators_in_price_range(
                exchange=ob_exchange,
                symbol=ob_symbol,
                min_price=min_price,
                max_price=max_price,
                timeframes=higher_timeframes
            )
            
            # Convert to DTOs if necessary
            mtf_order_blocks = [OrderBlockDto.from_dict(ob) if isinstance(ob, dict) else ob 
                            for ob in mtf_order_blocks]
            
            # Count overlapping order blocks from higher timeframes with weighted scoring
            overlap_score = 0.0
            max_possible_score = 0.0
            
            for other_ob in mtf_order_blocks:
                # Skip if not in higher timeframes list
                if other_ob.timeframe not in higher_timeframes:
                    continue
                # Calculate weight based on position in the hierarchy
                # Higher timeframes get higher weights
                tf_index = higher_timeframes.index(other_ob.timeframe)
                tf_position = tf_index / max(1, len(higher_timeframes) - 1)
                tf_weight = 0.4 + 0.6 * (tf_position ** 2)

                max_possible_score += tf_weight

                # Calculate price overlap percentage
                overlap_low = max(ob_low, other_ob.price_low)
                overlap_high = min(ob_high, other_ob.price_high)

                if overlap_high > overlap_low:
                    current_range = ob_high - ob_low
                    other_range = other_ob.price_high - other_ob.price_low
                    overlap_range = overlap_high - overlap_low

                    reference_range = min(current_range, other_range)
                    overlap_percentage = overlap_range / reference_range if reference_range > 0 else 0

                    overlap_score += tf_weight * overlap_percentage

                    # Add weighted score based on timeframe and overlap
                    overlap_score += (1.0 + tf_weight) * overlap_percentage
            
            # Normalize the overlap score (cap at 1.0)
            if max_possible_score > 0:
                mtf_score = min(overlap_score / max_possible_score, 1.0)
            else:
                mtf_score = 0.0
            
            return mtf_score
            
        except Exception as e:
            logger.error(f"Error calculating multi-timeframe confluence: {e}")
            return 0.0
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Get the data requirements for this strategy
        
        Returns:
            Dictionary with requirements
        """
        return {
            'lookback_period': 50,
            'timeframes': ['15m', '1h', '4h', '1d'],
            'indicators': ['order_block', 'fvg', 'structure_break', 'doji_candle']
        }