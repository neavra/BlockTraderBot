from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from strategy.indicators.base import Indicator
from strategy.dto.order_block_dto import OrderBlockDto, OrderBlockResultDto
import logging

logger = logging.getLogger(__name__)

class OrderBlockIndicator(Indicator):
    """
    Composite indicator that detects demand and supply order blocks based on
    multiple technical patterns: Doji candles, Fair Value Gaps (FVG), and Breaking of Structure (BOS).
    
    An order block is characterized by this specific sequence:
    1. A doji candle with specific characteristics (short body, long wicks)
    2. Followed by an imbalance/FVG (Fair Value Gap) in the opposite direction
    3. A break of structure confirming the direction
    
    Types of order blocks:
    - Demand Order Block (Bullish): Bearish (red) doji candle followed by bullish FVG and bullish BOS
    - Supply Order Block (Bearish): Bullish (green) doji candle followed by bearish FVG and bearish BOS
    
    If multiple doji candles exist before an FVG, the later one is selected.
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize order block detector with parameters
        
        Args:
            params: Dictionary containing parameters:
                - max_body_to_range_ratio: Maximum ratio of body to total range
                - min_wick_to_body_ratio: Minimum ratio of wicks to body
                - lookback_period: Number of candles to look back
                - max_detection_window: Max candles to look ahead for FVG and BOS after doji
                - require_doji: Whether a doji pattern is required for order block (default: True)
                - require_bos: Whether a break of structure is required for order block (default: True)
        """
        default_params = {
            'max_body_to_range_ratio': 0.4,   # Maximum ratio of body to total range
            'min_wick_to_body_ratio': 1.5,    # Minimum ratio of wicks to body
            'lookback_period': 50,            # Candles to analyze
            'max_detection_window': 5,        # Max candles to look ahead for FVG/BOS after doji
            'require_doji': True,             # Whether a doji pattern is required (enforced)
            'require_bos': True,              # Whether a break of structure is required (enforced)
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> OrderBlockResultDto:
        """
        Detect order blocks in the provided candle data by combining insights from
        doji candles, FVG, and BOS indicators.
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles
                - symbol: Trading symbol
                - timeframe: Timeframe
                - fvg_data: FVG indicator results (from FVGIndicator)
                - doji_data: Doji indicator results (from DojiCandleIndicator)
                - bos_data: BOS indicator results (from StructureBreakIndicator)
                
        Returns:
            OrderBlockResultDto with detected order blocks
        """
        candles = data.get('candles', [])
        fvg_data = data.get('fvg_data', {})
        doji_data = data.get('doji_data', {})
        bos_data = data.get('bos_data', {})
        
        # Need enough candles to detect order blocks
        if len(candles) < 5:
            logger.warning("Not enough candles to detect order blocks")
            return self._get_empty_result()
        
        # Check if we have doji data (required)
        if not doji_data:
            logger.warning("No doji data provided, order blocks require doji candles")
            return self._get_empty_result()
            
        # Get dojis, handling both DTO and dictionary formats
        if hasattr(doji_data, 'dojis'):
            # DTO format
            doji_candles = doji_data.dojis
            # Convert to dictionaries for compatibility if needed
            doji_candles = [doji.to_dict() if hasattr(doji, 'to_dict') else doji for doji in doji_candles]
        else:
            # Dictionary format
            doji_candles = doji_data.get('dojis', [])
            
        if not doji_candles:
            logger.warning("No doji candles detected, order blocks require doji candles")
            return self._get_empty_result()
        
        # Get FVG data, handling both DTO and dictionary formats
        if hasattr(fvg_data, 'bullish_fvgs') and hasattr(fvg_data, 'bearish_fvgs'):
            # DTO format
            bullish_fvgs = fvg_data.bullish_fvgs
            bearish_fvgs = fvg_data.bearish_fvgs
            # Convert to dictionaries for compatibility
            bullish_fvgs = [fvg.to_dict() if hasattr(fvg, 'to_dict') else fvg for fvg in bullish_fvgs]
            bearish_fvgs = [fvg.to_dict() if hasattr(fvg, 'to_dict') else fvg for fvg in bearish_fvgs]
        else:
            # Dictionary format
            bullish_fvgs = fvg_data.get('bullish_fvgs', [])
            bearish_fvgs = fvg_data.get('bearish_fvgs', [])
        
        # Get BOS data, handling both DTO and dictionary formats
        if hasattr(bos_data, 'bullish_breaks') and hasattr(bos_data, 'bearish_breaks'):
            # New DTO format
            bullish_bos = bos_data.bullish_breaks
            bearish_bos = bos_data.bearish_breaks
            # Convert to dictionaries for compatibility
            bullish_bos = [bos.to_dict() if hasattr(bos, 'to_dict') else bos for bos in bullish_bos]
            bearish_bos = [bos.to_dict() if hasattr(bos, 'to_dict') else bos for bos in bearish_bos]
            bos_events = bullish_bos + bearish_bos
        elif hasattr(bos_data, 'breaks'):
            # Old DTO format with a single 'breaks' list
            bos_events = bos_data.breaks
            # Convert to dictionaries for compatibility
            bos_events = [bos.to_dict() if hasattr(bos, 'to_dict') else bos for bos in bos_events]
        else:
            # Dictionary format
            bos_events = bos_data.get('breaks', []) if bos_data else []
        
        # Detect order blocks based on composite analysis with the specific sequence:
        # Doji candle -> FVG -> BOS
        demand_blocks, supply_blocks = self._detect_order_blocks(
            candles, doji_candles, bullish_fvgs, bearish_fvgs, bos_events
        )
        
        # Sort blocks by index (most recent first)
        demand_blocks.sort(key=lambda x: x.index, reverse=True)
        supply_blocks.sort(key=lambda x: x.index, reverse=True)
        
        # Create and return the result DTO
        return OrderBlockResultDto(
            timestamp=datetime.now(timezone.utc),
            indicator_name="OrderBlock",
            demand_blocks=demand_blocks,
            supply_blocks=supply_blocks
        )
    
    def _detect_order_blocks(self, 
                             candles: List[Dict[str, Any]],
                             doji_candles: List[Dict[str, Any]],
                             bullish_fvgs: List[Dict[str, Any]],
                             bearish_fvgs: List[Dict[str, Any]],
                             bos_events: List[Dict[str, Any]]) -> Tuple[List[OrderBlockDto], List[OrderBlockDto]]:
        """
        Core order block detection logic, following the specific sequence:
        1. Identify doji candles
        2. For each doji, look for subsequent FVG within the detection window
        3. For each doji+FVG pair, look for subsequent BOS within the detection window
        
        Args:
            candles: List of OHLCV candles
            doji_candles: List of doji candles
            bullish_fvgs: List of bullish FVGs
            bearish_fvgs: List of bearish FVGs
            bos_events: List of breaking of structure events
            
        Returns:
            Tuple of (demand order blocks, supply order blocks)
        """
        demand_blocks = []  # Bullish order blocks
        supply_blocks = []  # Bearish order blocks
        
        # Extract parameters
        max_detection_window = self.params['max_detection_window']
        require_bos = self.params['require_bos']
        
        # Group BOS events by type for efficient lookup
        bullish_bos = [b for b in bos_events if b.get('break_type') in ['higher_high', 'higher_low']]
        bearish_bos = [b for b in bos_events if b.get('break_type') in ['lower_low', 'lower_high']]
                
        # Group dojis by direction (bearish/bullish)
        bearish_dojis = []  # Red dojis
        bullish_dojis = []  # Green dojis
        
        for doji in doji_candles:
            doji_idx = doji['index']
            if doji_idx >= len(candles) or doji_idx < 0:
                continue  # Skip invalid indices
                
            doji_candle = doji['candle']
            # Determine doji direction (bearish/red or bullish/green)
            if doji_candle['close'] < doji_candle['open']:
                bearish_dojis.append(doji)
            elif doji_candle['close'] > doji_candle['open']:
                bullish_dojis.append(doji)
        
        # Sort dojis by index (most recent first for later selection when multiple dojis exist)
        bearish_dojis.sort(key=lambda x: x['index'], reverse=True)
        bullish_dojis.sort(key=lambda x: x['index'], reverse=True)
        
        # Process demand order blocks (bearish doji -> bullish FVG -> bullish BOS)
        processed_fvgs = set()  # Track processed FVGs to avoid duplicates
        
        for doji in bearish_dojis:
            doji_idx = doji['index']
            doji_candle = doji['candle']
            
            # Find bullish FVGs within the detection window after this doji
            for fvg in bullish_fvgs:
                fvg_idx = fvg.get('candle_index', 0)
                
                # Check if FVG is within detection window after the doji
                # and hasn't been processed yet
                fvg_id = f"{fvg_idx}_{fvg.get('top', 0)}_{fvg.get('bottom', 0)}"
                if 0 < fvg_idx - doji_idx <= max_detection_window and fvg_id not in processed_fvgs:
                    # If BOS is required, check for a bullish BOS after the FVG
                    bos_found = False if require_bos else True
                    bos_data = None
                    
                    if require_bos:
                        for bos in bullish_bos:
                            bos_idx = bos.get('index', 0)
                            # BOS should occur within the detection window after the FVG
                            if 0 < bos_idx - fvg_idx <= max_detection_window:
                                bos_found = True
                                bos_data = bos
                                break
                    
                    if bos_found:
                        # We found a complete demand order block pattern
                        # (bearish doji -> bullish FVG -> bullish BOS)
                        timestamp = doji_candle.get('timestamp')
                        
                        demand_block = OrderBlockDto(
                            type='demand',
                            price_high=doji_candle['open'],
                            price_low=doji_candle['close'],
                            index=doji_idx,
                            wick_ratio=doji.get('wick_ratio', 0.0),
                            body_ratio=doji.get('body_ratio', 0.0),
                            candle=doji_candle.copy(),
                            is_doji=True,
                            timestamp=timestamp,
                            doji_data=doji,
                            # Store related data in candle field or extend the DTO to include these fields
                            related_fvg=fvg,
                            bos_data=bos_data
                        )
                        
                        demand_blocks.append(demand_block)
                        processed_fvgs.add(fvg_id)  # Mark this FVG as processed
                        break  # Found a valid pattern, move to next doji
        
        # Process supply order blocks (bullish doji -> bearish FVG -> bearish BOS)
        processed_fvgs = set()  # Reset processed FVGs for supply blocks
        
        for doji in bullish_dojis:
            doji_idx = doji['index']
            doji_candle = doji['candle']
            
            # Find bearish FVGs within the detection window after this doji
            for fvg in bearish_fvgs:
                fvg_idx = fvg.get('candle_index', 0)
                
                # Check if FVG is within detection window after the doji
                # and hasn't been processed yet
                fvg_id = f"{fvg_idx}_{fvg.get('top', 0)}_{fvg.get('bottom', 0)}"
                if 0 < fvg_idx - doji_idx <= max_detection_window and fvg_id not in processed_fvgs:
                    # If BOS is required, check for a bearish BOS after the FVG
                    bos_found = False if require_bos else True
                    bos_data = None
                    
                    if require_bos:
                        for bos in bearish_bos:
                            bos_idx = bos.get('index', 0)
                            # BOS should occur within the detection window after the FVG
                            if 0 < bos_idx - fvg_idx <= max_detection_window:
                                bos_found = True
                                bos_data = bos
                                break
                    
                    if bos_found:
                        # We found a complete supply order block pattern
                        # (bullish doji -> bearish FVG -> bearish BOS)
                        timestamp = doji_candle.get('timestamp')
                        
                        supply_block = OrderBlockDto(
                            type='supply',
                            price_high=doji_candle['close'],
                            price_low=doji_candle['open'],
                            index=doji_idx,
                            wick_ratio=doji.get('wick_ratio', 0.0),
                            body_ratio=doji.get('body_ratio', 0.0),
                            candle=doji_candle.copy(),
                            is_doji=True,
                            timestamp=timestamp,
                            doji_data=doji,
                            # Store related data in candle field or extend the DTO to include these fields
                            related_fvg=fvg
                        )
                        
                        supply_blocks.append(supply_block)
                        processed_fvgs.add(fvg_id)  # Mark this FVG as processed
                        break  # Found a valid pattern, move to next doji
        
        return demand_blocks, supply_blocks
    
    def _get_empty_result(self) -> OrderBlockResultDto:
        """Return an empty result structure when no order blocks can be detected"""
        return OrderBlockResultDto(
            timestamp=datetime.now(timezone.utc),
            indicator_name="OrderBlock",
            demand_blocks=[],
            supply_blocks=[]
        )
    
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
            'indicators': ['structure_break', 'fvg', 'doji_candle']
        }