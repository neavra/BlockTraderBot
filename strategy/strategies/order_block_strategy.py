from typing import Dict, Any, Optional, List
from strategy.strategies.base import Strategy
from strategy.indicators.base import Indicator
from shared.domain.dto.signal_dto import SignalDto
from strategy.domain.models.market_context import MarketContext
from strategy.domain.types.indicator_type_enum import IndicatorType
from strategy.domain.dto.strength_dto import StrengthDto
from strategy.domain.dto.order_block_dto import OrderBlockDto, OrderBlockResultDto

TIMEFRAME_HIERARCHY: Dict[str, List[str]] = {
        "1m": ["1m", "5m", "15m"],
        "5m": ["5m", "15m", "1h"],
        "15m": ["15m", "1h", "4h"],
        "30m": ["30m", "1h", "4h"],
        "1h": ["1h", "4h", "1d"],
        "2h": ["2h", "4h", "1d"],
        "4h": ["4h", "1d", "1w"],
        "1d": ["1d", "1w"],
        "1w": ["1w", "1M"],
        "1M": ["1M"]
    }

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
            'max_signals_per_day': 3
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
            results: StrengthDto = self.calculate_strength(block, market_contexts, demand_blocks + supply_blocks)
            block.strength = results.overall_score
            swing_score = results.swing_proximity
            fib_score = results.fib_confluence
            mtf_score = results.mtf_confluence
            
            # Check if strength exceeds threshold
            if block.strength < self.params['strength_threshold']:
                continue
            
            # Calculate trigger price (slightly above the order block)
            trigger_price = block.price_low * 0.995  # 0.5% below low
            
            # Calculate stop loss (below the order block low)
            stop_loss = block.price_low * 0.98  # 2% below low
            
            # Calculate take profit based on risk:reward ratio
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
            if self._validate_signal(signal, market_contexts):
                signals.append(signal)
        
        # Process supply (bearish) order blocks
        # for block in supply_blocks:
        #     # Similar processing as demand blocks, but for short positions
        #     # ...
            
        
        return signals

    def _calculate_position_size(self, entry_price, stop_loss, risk_percentage):
        """Calculate position size based on risk management rules"""
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

    def _validate_signal(self, signal, market_context):
        """Validate if a signal should be executed"""
        # Check for existing active signals on this symbol
        # active_signals = self._get_active_signals(signal.symbol)
        # if len(active_signals) >= self.params['max_signals_per_symbol']:
        #     return False
        
        # # Check for trend alignment (optional)
        # if self.params.get('require_trend_alignment', False):
        #     trend = market_context.trend
        #     if (signal.direction == 'long' and trend != 'up') or \
        #     (signal.direction == 'short' and trend != 'down'):
        #         return False
        
        # # Check for minimum R:R ratio
        # min_rr = self.params.get('min_risk_reward_ratio', 1.5)
        # if signal.risk_reward_ratio < min_rr:
        #     return False
        
        
        return True
    
    def calculate_strength(self, order_block: OrderBlockDto, market_contexts: List[MarketContext], all_order_blocks: List[OrderBlockDto]) -> StrengthDto:
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
        mtf_score = self.calculate_mtf_confluence(order_block, all_order_blocks)
        
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

    # def calculate_swing_proximity(order_block: OrderBlockDto, market_contexts: List[MarketContext]):
    #     # Get swing high and low from market context
    #     swing_high = market_context.swing_high.get('price')
    #     swing_low = market_context.swing_low.get('price')
        
    #     # For demand (bullish) order blocks, proximity to swing low matters more
    #     if order_block.type == 'demand':
    #         # Calculate distance as percentage of price
    #         distance = abs(order_block.price_low - swing_low) / swing_low
    #         # Convert to proximity (closer = higher score)
    #         proximity = max(0, 1 - min(distance / 0.05, 1))  # Within 5% is full strength
        
    #     # For supply (bearish) order blocks, proximity to swing high matters more
    #     else:
    #         # Calculate distance as percentage of price
    #         distance = abs(order_block.price_high - swing_high) / swing_high
    #         # Convert to proximity (closer = higher score)
    #         proximity = max(0, 1 - min(distance / 0.05, 1))  # Within 5% is full strength
        
    #     return proximity

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
                    tf_weight = 0.4 + (0.6 * tf_position ** 2)  # Scale from 0.4 to 1.0 with exponential increase
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
                    tf_weight = 0.4 + (0.6 * tf_position ** 2)  # Scale from 0.4 to 1.0 with exponential increase
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
    
    def calculate_mtf_confluence(self, order_block: OrderBlockDto, all_order_blocks: List[OrderBlockDto]):
        # Get current order block's price range
        ob_high = order_block.price_high
        ob_low = order_block.price_low
        ob_timeframe = order_block.timeframe
        
        # Count overlapping order blocks from higher timeframes
        overlap_count = 0
        higher_timeframes = ['1h', '4h', '1d', '1w'] # Define your timeframe hierarchy
        current_tf_index = higher_timeframes.index(ob_timeframe) if ob_timeframe in higher_timeframes else -1
        
        for other_ob in all_order_blocks:
            # Skip if same order block or lower timeframe
            other_tf = other_ob.timeframe
            other_tf_index = higher_timeframes.index(other_tf) if other_tf in higher_timeframes else -1
            
            if other_tf_index <= current_tf_index:
                continue
                
            # Check for price overlap
            overlap = (
                (ob_low <= other_ob.price_high and ob_high >= other_ob.price_low) or
                (other_ob.price_low <= ob_high and other_ob.price_high >= ob_low)
            )
            
            if overlap:
                # Higher timeframe confluences are more valuable
                tf_weight = (other_tf_index - current_tf_index) / len(higher_timeframes)
                overlap_count += (1 + tf_weight)
        
        # Normalize the overlap count (max expected value is 3)
        mtf_score = min(overlap_count / 3, 1.0)
        
        return mtf_score
    
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