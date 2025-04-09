from typing import Dict, Any, List, Tuple, Optional
from strategy.indicators.base import Indicator
import logging

logger = logging.getLogger(__name__)

class OrderBlockIndicator(Indicator):
    """
    Composite indicator that detects demand and supply order blocks based on
    multiple technical patterns: Doji candles, Fair Value Gaps (FVG), and Breaking of Structure (BOS).
    
    An order block is characterized by:
    1. A candle with specific characteristics (often a doji or pin bar)
    2. Followed by an imbalance/FVG (Fair Value Gap)
    3. A break of structure confirming the direction
    
    Types of order blocks:
    - Demand Order Block (Bullish): Bearish candle followed by bullish FVG and bullish BOS
    - Supply Order Block (Bearish): Bullish candle followed by bearish FVG and bearish BOS
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize order block detector with parameters
        
        Args:
            params: Dictionary containing parameters:
                - max_body_to_range_ratio: Maximum ratio of body to total range
                - min_wick_to_body_ratio: Minimum ratio of wicks to body
                - lookback_period: Number of candles to look back
                - max_ob_detection_candles: Max candles to look after potential OB for FVG
                - require_doji: Whether a doji pattern is required for order block
                - require_bos: Whether a break of structure is required for order block
        """
        default_params = {
            'max_body_to_range_ratio': 0.4,   # Maximum ratio of body to total range
            'min_wick_to_body_ratio': 1.5,    # Minimum ratio of wicks to body
            'lookback_period': 50,            # Candles to analyze
            'max_ob_detection_candles': 5,    # Max candles to look ahead for FVG after potential OB
            'require_doji': False,            # Whether a doji pattern is required
            'require_bos': False,             # Whether a break of structure is required
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect order blocks in the provided candle data by combining insights from
        doji candles, FVG, and BOS indicators.
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles
                - symbol: Trading symbol
                - timeframe: Timeframe
                - fvg_data: FVG indicator results (from FVGIndicator)
                - doji_data: Doji indicator results (from DojiCandleIndicator) (optional)
                - bos_data: BOS indicator results (from StructureBreakIndicator) (optional)
                
        Returns:
            Dictionary with detected order blocks:
                - blocks: List of all order blocks (both demand and supply)
                - demand_blocks: List of demand order blocks
                - supply_blocks: List of supply order blocks
                - has_demand_block: Boolean indicating if demand blocks were found
                - has_supply_block: Boolean indicating if supply blocks were found
                - latest_block: Most recent order block or None
        """
        candles = data.get('candles', [])
        fvg_data = data.get('fvg_data', {})
        doji_data = data.get('doji_data', {})
        bos_data = data.get('bos_data', {})
        
        # Need enough candles to detect order blocks
        if len(candles) < 5:
            logger.warning("Not enough candles to detect order blocks")
            return self._get_empty_result()
        
        # Get FVGs from data
        bullish_fvgs = fvg_data.get('bullish_fvgs', [])
        bearish_fvgs = fvg_data.get('bearish_fvgs', [])
        
        # Get Doji candles if available
        doji_candles = doji_data.get('dojis', []) if doji_data else []
        
        # Get BOS events if available
        bos_events = bos_data.get('breaks', []) if bos_data else []
        
        # Detect order blocks based on composite analysis
        demand_blocks, supply_blocks = self._detect_order_blocks(
            candles, bullish_fvgs, bearish_fvgs, doji_candles, bos_events
        )
        
        # Combine all blocks into a single list
        all_blocks = demand_blocks + supply_blocks
        
        # Sort by index (most recent first)
        all_blocks.sort(key=lambda x: x['index'], reverse=True)
        
        # Get latest block if any
        latest_block = all_blocks[0] if all_blocks else None
        
        # Prepare result
        result = {
            'blocks': all_blocks,
            'demand_blocks': demand_blocks,
            'supply_blocks': supply_blocks,
            'has_demand_block': len(demand_blocks) > 0,
            'has_supply_block': len(supply_blocks) > 0,
            'latest_block': latest_block
        }
        
        return result
    
    def _detect_order_blocks(self, 
                            candles: List[Dict[str, Any]], 
                            bullish_fvgs: List[Dict[str, Any]],
                            bearish_fvgs: List[Dict[str, Any]],
                            doji_candles: List[Dict[str, Any]],
                            bos_events: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Core order block detection logic
        
        Args:
            candles: List of OHLCV candles
            bullish_fvgs: List of bullish FVGs
            bearish_fvgs: List of bearish FVGs
            doji_candles: List of doji candles
            bos_events: List of breaking of structure events
            
        Returns:
            Tuple of (demand order blocks, supply order blocks)
        """
        demand_blocks = []  # Bullish order blocks
        supply_blocks = []  # Bearish order blocks
        
        # Extract parameters
        max_body_to_range_ratio = self.params['max_body_to_range_ratio']
        min_wick_to_body_ratio = self.params['min_wick_to_body_ratio']
        lookback_period = min(self.params['lookback_period'], len(candles))
        max_ob_detection_candles = self.params['max_ob_detection_candles']
        require_doji = self.params['require_doji']
        require_bos = self.params['require_bos']
        
        # Create lookup dictionaries for easy access
        doji_lookup = {d['index']: d for d in doji_candles} if doji_candles else {}
        
        # Group BOS events by type for efficient lookup
        bullish_bos = [b for b in bos_events if b['break_type'] in ['higher_high', 'higher_low']]
        bearish_bos = [b for b in bos_events if b['break_type'] in ['lower_low', 'lower_high']]
        
        # Iterate through candles to find potential order blocks
        for i in range(lookback_period):
            current_idx = len(candles) - i - 1
            if current_idx < 0:
                continue
                
            current_candle = candles[current_idx]
            
            # Skip this candle if doji is required and this is not a doji
            if require_doji and current_idx not in doji_lookup:
                continue
            
            # Calculate body and wick properties
            body_range = abs(current_candle['close'] - current_candle['open'])
            total_range = current_candle['high'] - current_candle['low']
            
            # Avoid division by zero
            if body_range == 0:
                body_range = 0.0001
                
            # Calculate ratios
            body_to_range_ratio = body_range / total_range
            upper_wick = max(current_candle['high'] - current_candle['open'], 
                            current_candle['high'] - current_candle['close'])
            lower_wick = max(current_candle['open'] - current_candle['low'], 
                           current_candle['close'] - current_candle['low'])
            total_wick = upper_wick + lower_wick
            wick_to_body_ratio = total_wick / body_range
            
            # Check if this candle has short body and long wicks
            if body_to_range_ratio <= max_body_to_range_ratio and wick_to_body_ratio >= min_wick_to_body_ratio:
                # Determine candle direction
                is_bearish = current_candle['close'] < current_candle['open']
                is_bullish = current_candle['close'] > current_candle['open']
                
                # Check for demand order blocks (bearish candle before bullish FVG)
                if is_bearish:
                    for fvg in bullish_fvgs:
                        fvg_idx = fvg.get('candle_index', 0)
                        # Check if FVG is within max_ob_detection_candles after the potential OB
                        if 0 < fvg_idx - current_idx <= max_ob_detection_candles:
                            # If BOS is required, check for a bullish BOS after FVG
                            if require_bos:
                                # Find a bullish BOS that occurs after the FVG
                                found_bos = False
                                for bos in bullish_bos:
                                    if bos['index'] > fvg_idx:
                                        found_bos = True
                                        break
                                        
                                if not found_bos:
                                    continue  # Skip if no BOS found and it's required
                            
                            # Create demand order block
                            demand_block = {
                                'type': 'demand',
                                'price_high': current_candle['open'],
                                'price_low': current_candle['close'],
                                'index': current_idx,
                                'wick_ratio': wick_to_body_ratio,
                                'body_ratio': body_to_range_ratio,
                                'candle': current_candle.copy(),
                                'related_fvg': fvg,
                                'is_doji': current_idx in doji_lookup,
                            }
                            
                            # Add doji data if it's a doji
                            if current_idx in doji_lookup:
                                demand_block['doji_data'] = doji_lookup[current_idx]
                            
                            # Add timestamp if available
                            if 'timestamp' in current_candle:
                                demand_block['timestamp'] = current_candle['timestamp']
                            
                            demand_blocks.append(demand_block)
                            break  # Found an FVG for this order block
                
                # Check for supply order blocks (bullish candle before bearish FVG)
                elif is_bullish:
                    for fvg in bearish_fvgs:
                        fvg_idx = fvg.get('candle_index', 0)
                        # Check if FVG is within max_ob_detection_candles after the potential OB
                        if 0 < fvg_idx - current_idx <= max_ob_detection_candles:
                            # If BOS is required, check for a bearish BOS after FVG
                            if require_bos:
                                # Find a bearish BOS that occurs after the FVG
                                found_bos = False
                                for bos in bearish_bos:
                                    if bos['index'] > fvg_idx:
                                        found_bos = True
                                        break
                                        
                                if not found_bos:
                                    continue  # Skip if no BOS found and it's required
                            
                            # Create supply order block
                            supply_block = {
                                'type': 'supply',
                                'price_high': current_candle['close'],
                                'price_low': current_candle['open'],
                                'index': current_idx,
                                'wick_ratio': wick_to_body_ratio,
                                'body_ratio': body_to_range_ratio,
                                'candle': current_candle.copy(),
                                'related_fvg': fvg,
                                'is_doji': current_idx in doji_lookup,
                            }
                            
                            # Add doji data if it's a doji
                            if current_idx in doji_lookup:
                                supply_block['doji_data'] = doji_lookup[current_idx]
                            
                            # Add timestamp if available
                            if 'timestamp' in current_candle:
                                supply_block['timestamp'] = current_candle['timestamp']
                            
                            supply_blocks.append(supply_block)
                            break  # Found an FVG for this order block
        
        return demand_blocks, supply_blocks
    
    def _get_empty_result(self) -> Dict[str, Any]:
        """Return an empty result structure when no order blocks can be detected"""
        return {
            'blocks': [],
            'demand_blocks': [],
            'supply_blocks': [],
            'has_demand_block': False,
            'has_supply_block': False,
            'latest_block': None
        }
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns data requirements for order block detection
        
        Returns:
            Dictionary with requirements
        """
        return {
            'candles': True,
            'lookback_period': 50,
            'timeframes': ['15m', '1h', '4h', '1d'],
            'indicators': ['structure_break', 'fvg', 'doji_candle']
        }