from typing import Dict, Any, List, Tuple, Optional
from .base import Indicator
import logging
import numpy as np

logger = logging.getLogger(__name__)

class OrderBlockIndicator(Indicator):
    """
    Indicator that detects demand and supply order blocks
    
    An order block is characterized by:
    1. Short body and relatively long wicks (one or more candles)
    2. Followed by an imbalance/FVG (Fair Value Gap)
    3. Optionally followed by a break of structure
    
    - Demand Order Block (Bullish): Bearish candle(s) before bullish FVG
    - Supply Order Block (Bearish): Bullish candle(s) before bearish FVG
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
        """
        default_params = {
            'max_body_to_range_ratio': 0.4,   # Maximum ratio of body to total range
            'min_wick_to_body_ratio': 1.5,    # Minimum ratio of wicks to body
            'lookback_period': 50,            # Candles to analyze
            'max_ob_detection_candles': 5,    # Max candles to look ahead for FVG after potential OB
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect order blocks in the provided candle data
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles
                - symbol: Trading symbol
                - timeframe: Timeframe
                - fvg_data: FVG indicator results (containing bullish_fvgs and bearish_fvgs)
                
        Returns:
            Dictionary with detected order blocks:
                - demand_blocks: List of demand order blocks
                - supply_blocks: List of supply order blocks
        """
        candles = data.get('candles', [])
        fvg_data = data.get('fvg_data', {})
        
        # Need enough candles to detect order blocks
        if len(candles) < 5:
            logger.warning("Not enough candles to detect order blocks")
            return {
                'demand_blocks': [],
                'supply_blocks': []
            }
        
        # Get FVGs from data
        bullish_fvgs = fvg_data.get('bullish_fvgs', [])
        bearish_fvgs = fvg_data.get('bearish_fvgs', [])
        
        # Detect order blocks based on price action patterns and FVGs
        demand_blocks, supply_blocks = self._detect_order_blocks(candles, bullish_fvgs, bearish_fvgs)
        
        # Prepare result
        result = {
            'demand_blocks': demand_blocks,
            'supply_blocks': supply_blocks
        }
        
        return result
    
    def _detect_order_blocks(self, candles: List[Dict[str, Any]], 
                            bullish_fvgs: List[Dict[str, Any]],
                            bearish_fvgs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Core order block detection logic
        
        Args:
            candles: List of OHLCV candles
            bullish_fvgs: List of bullish FVGs
            bearish_fvgs: List of bearish FVGs
            
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
        
        # Iterate through candles to find potential order blocks
        for i in range(lookback_period):
            current_idx = len(candles) - i - 1
            if current_idx < 0:
                continue
                
            current_candle = candles[current_idx]
            
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
                
                # For each potential order block, check if it precedes an FVG within max_ob_detection_candles
                
                # Check for demand order blocks (bearish candle before bullish FVG)
                if is_bearish:
                    for fvg in bullish_fvgs:
                        fvg_idx = fvg.get('candle_index', 0)
                        # Check if FVG is within max_ob_detection_candles after the potential OB
                        if 0 < fvg_idx - current_idx <= max_ob_detection_candles:
                            # Create demand order block
                            demand_block = {
                                'type': 'demand',
                                'price_high': current_candle['open'],
                                'price_low': current_candle['close'],
                                'index': current_idx,
                                'wick_ratio': wick_to_body_ratio,
                                'body_ratio': body_to_range_ratio,
                                'related_fvg': fvg
                            }
                            
                            # Add timestamp if available
                            if 'timestamp' in current_candle:
                                demand_block['timestamp'] = current_candle['timestamp']
                            
                            # Check for break of structure (optional)
                            # if self._check_bullish_bos(candles, current_idx, fvg_idx):
                            #     demand_block['has_bos'] = True
                            
                            demand_blocks.append(demand_block)
                            break  # Found an FVG for this order block
                
                # Check for supply order blocks (bullish candle before bearish FVG)
                elif is_bullish:
                    for fvg in bearish_fvgs:
                        fvg_idx = fvg.get('candle_index', 0)
                        # Check if FVG is within max_ob_detection_candles after the potential OB
                        if 0 < fvg_idx - current_idx <= max_ob_detection_candles:
                            # Create supply order block
                            supply_block = {
                                'type': 'supply',
                                'price_high': current_candle['close'],
                                'price_low': current_candle['open'],
                                'index': current_idx,
                                'wick_ratio': wick_to_body_ratio,
                                'body_ratio': body_to_range_ratio,
                                'related_fvg': fvg
                            }
                            
                            # Add timestamp if available
                            if 'timestamp' in current_candle:
                                supply_block['timestamp'] = current_candle['timestamp']
                            
                            # if self._check_bearish_bos(candles, current_idx, fvg_idx):
                            #     supply_block['has_bos'] = True
                            
                            supply_blocks.append(supply_block)
                            break  # Found an FVG for this order block
        
        return demand_blocks, supply_blocks
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns data requirements for order block detection
        
        Returns:
            Dictionary with requirements
        """
        return {
            'candles': True,
            'lookback_period': self.params['lookback_period'],
            'timeframes': ['15m', '1h', '4h', '1d'],
            'indicators': ['fvg']  # Now requires FVG indicator
        }