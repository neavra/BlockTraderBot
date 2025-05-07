from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from strategy.indicators.base import Indicator
from strategy.domain.dto.order_block_dto import OrderBlockDto, OrderBlockResultDto
import logging
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.dto.bos_dto import StructureBreakDto, StructureBreakResultDto
from strategy.domain.dto.fvg_dto import FvgDto, FvgResultDto
from strategy.domain.dto.doji_dto import DojiDto, DojiResultDto
from strategy.domain.types.indicator_type_enum import IndicatorType
from data.database.repository.order_block_repository import OrderBlockRepository


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
    
    def __init__(self, repository: OrderBlockRepository, params: Dict[str, Any] = None):
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
                - mitigation_threshold: Percentage of zone that must be mitigated to invalidate (default: 0.5)
        """

        self.repository = repository

        default_params = {
            'max_body_to_range_ratio': 0.4,   # Maximum ratio of body to total range
            'min_wick_to_body_ratio': 1.5,    # Minimum ratio of wicks to body
            'lookback_period': 50,            # Candles to analyze
            'max_detection_window': 5,        # Max candles to look ahead for FVG/BOS after doji
            'require_doji': True,             # Whether a doji pattern is required (enforced)
            'require_bos': True,              # Whether a break of structure is required (enforced)
            'mitigation_threshold': 0.5,      # 50% zone mitigation invalidates the order block
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> OrderBlockResultDto:
        """
        Detect order blocks in the provided candle data by combining insights from
        doji candles, FVG, and BOS indicators, and save them to the database.
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles
                - symbol: Trading symbol
                - timeframe: Timeframe
                - exchange: Exchange name
                - fvg_data: FVG indicator results (from FVGIndicator)
                - doji_data: Doji indicator results (from DojiCandleIndicator)
                - bos_data: BOS indicator results (from StructureBreakIndicator)
                
        Returns:
            OrderBlockResultDto with detected order blocks
        """
        candles: List[CandleDto] = data.get("candles")
        doji_data: DojiResultDto = data.get("doji_candle_data")
        fvg_data: FvgResultDto = data.get("fvg_data")
        bos_data: StructureBreakResultDto = data.get("structure_break_data")
        
        # Extract market data information
        symbol = data.get("symbol")
        timeframe = data.get("timeframe")
        exchange = data.get("exchange", "default")
        
        # Need enough candles to detect order blocks
        if len(candles) < 5:
            logger.warning("Not enough candles to detect order blocks")
            return self._get_empty_result()
        
        # Check if we have doji data (required)
        if not doji_data:
            logger.warning("No doji data provided, order blocks require doji candles")
            return self._get_empty_result()
        doji_candles = doji_data.dojis
        
        # Get FVG data
        if not fvg_data:
            logger.warning("No fvg data provided, order blocks require fvg candles")
            return self._get_empty_result()
        bullish_fvgs = fvg_data.bullish_fvgs
        bearish_fvgs = fvg_data.bearish_fvgs
        
        # Get BOS data
        if not bos_data:
            logger.warning("No bos data provided, order blocks require bos candles")
            return self._get_empty_result()
        bullish_bos = bos_data.bullish_breaks
        bearish_bos = bos_data.bearish_breaks

        # Detect order blocks based on composite analysis with the specific sequence:
        # Doji candle -> FVG -> BOS
        demand_blocks, supply_blocks = self._detect_order_blocks(
            candles, doji_candles, bullish_fvgs, bearish_fvgs, bullish_bos, bearish_bos
        )
        
        # Sort blocks by index (most recent first)
        demand_blocks.sort(key=lambda x: x.index, reverse=True)
        supply_blocks.sort(key=lambda x: x.index, reverse=True)
        
        # Save detected order blocks to the database
        try:
            # Prepare order blocks for database insertion
            order_blocks_data = []
            
            for block in demand_blocks + supply_blocks:
                # Skip blocks that might already exist in the database
                # You might want to add more sophisticated deduplication logic
                
                # Convert block to database format
                block_data = {
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "type": block.type,
                    "price_high": float(block.price_high),
                    "price_low": float(block.price_low),
                    "is_doji": block.is_doji,
                    "candle_index": block.index,
                    "timestamp": block.timestamp if isinstance(block.timestamp, datetime) else datetime.fromisoformat(block.timestamp),
                    "status": block.status,
                    "touched": block.touched,
                    "mitigation_percentage": float(block.mitigation_percentage),
                    
                    # Store the JSON-serializable versions of related data
                    "candle_data": self._serialize_candle(block.candle),
                    "doji_data": self._serialize_dto(block.doji_data),
                    "related_fvg": self._serialize_dto(block.related_fvg),
                    "bos_data": self._serialize_dto(block.bos_data),
                    
                    # Required fields from your model
                    "indicator_id": IndicatorType.ORDER_BLOCK.indicator_id,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                }
                
                order_blocks_data.append(block_data)
            
            # Bulk insert the order blocks
            if order_blocks_data:
                created_blocks = await self.repository.bulk_create_order_blocks(order_blocks_data)
                logger.info(f"Saved {len(created_blocks)} order blocks to database")
                
        except Exception as e:
            logger.error(f"Error saving order blocks to database: {str(e)}")
        
        # Create and return the result DTO
        return OrderBlockResultDto(
            timestamp=datetime.now(timezone.utc),
            indicator_name="OrderBlock",
            demand_blocks=demand_blocks,
            supply_blocks=supply_blocks
        )
    
    def _detect_order_blocks(self, 
                             candles: List[CandleDto],
                             doji_candles: List[DojiDto],
                             bullish_fvgs: List[FvgDto],
                             bearish_fvgs: List[FvgDto],
                             bullish_bos: List[StructureBreakDto],
                             bearish_bos: List[StructureBreakDto]
                             ) -> Tuple[List[OrderBlockDto], List[OrderBlockDto]]:
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
        
        # Group dojis by direction (bearish/bullish)
        bearish_dojis: List[DojiDto] = []  # Red dojis
        bullish_dojis: List[DojiDto] = []  # Green dojis
        
        for doji in doji_candles:
            doji_idx = doji.index
            if doji_idx >= len(candles) or doji_idx < 0:
                continue  # Skip invalid indices
                
            doji_candle = doji.candle
            # Determine doji direction (bearish/red or bullish/green)
            if doji_candle.close < doji_candle.open:
                bearish_dojis.append(doji)
            elif doji_candle.close > doji_candle.open:
                bullish_dojis.append(doji)
        
        # Process demand order blocks (bearish doji -> bullish FVG -> bullish BOS)
        processed_fvgs = set()  # Track processed FVGs to avoid duplicates
        
        for doji in bearish_dojis:
            doji_idx = doji.index
            doji_candle = doji.candle
            
            # Find bullish FVGs within the detection window after this doji
            for fvg in bullish_fvgs:
                fvg_idx = fvg.candle_index
                
                # Check if FVG is within detection window after the doji
                # and hasn't been processed yet
                fvg_id = f"{fvg_idx}_{fvg.top}_{fvg.bottom}"
                if 0 < fvg_idx - doji_idx <= max_detection_window and fvg_id not in processed_fvgs:
                    # If BOS is required, check for a bullish BOS after the FVG
                    bos_found = False if require_bos else True
                    bos_data = None
                    
                    if require_bos:
                        for bos in bullish_bos:
                            bos_idx = bos.index
                            # BOS should occur within the detection window after the FVG
                            if 0 < bos_idx - fvg_idx <= max_detection_window:
                                bos_found = True
                                bos_data = bos
                                break
                    
                    if bos_found:
                        # We found a complete demand order block pattern
                        # (bearish doji -> bullish FVG -> bullish BOS)
                        timestamp = doji_candle.timestamp
                        
                        demand_block = OrderBlockDto(
                            type='demand',
                            price_high=doji_candle.open,
                            price_low=doji_candle.close,
                            index=doji_idx,
                            candle=doji_candle,
                            is_doji=True,
                            timestamp=timestamp,
                            doji_data=doji,
                            # Store related data in candle field or extend the DTO to include these fields
                            related_fvg=fvg,
                            bos_data=bos_data,
                            # Add mitigation fields
                            status='active',
                            touched=False,
                            mitigation_percentage=0.0,
                            created_at=datetime.now(timezone.utc).isoformat()
                        )
                        
                        demand_blocks.append(demand_block)
                        processed_fvgs.add(fvg_id)  # Mark this FVG as processed
                        break  # Found a valid pattern, move to next doji
        
        # Process supply order blocks (bullish doji -> bearish FVG -> bearish BOS)
        processed_fvgs = set()  # Reset processed FVGs for supply blocks
        
        for doji in bullish_dojis:
            doji_idx = doji.index
            doji_candle = doji.candle
            
            # Find bearish FVGs within the detection window after this doji
            for fvg in bearish_fvgs:
                fvg_idx = fvg.candle_index
                
                # Check if FVG is within detection window after the doji
                # and hasn't been processed yet
                fvg_id = f"{fvg_idx}_{fvg.top}_{fvg.bottom}"
                if 0 < fvg_idx - doji_idx <= max_detection_window and fvg_id not in processed_fvgs:
                    # If BOS is required, check for a bearish BOS after the FVG
                    bos_found = False if require_bos else True
                    bos_data = None
                    
                    if require_bos:
                        for bos in bearish_bos:
                            bos_idx = bos.index
                            # BOS should occur within the detection window after the FVG
                            if 0 < bos_idx - fvg_idx <= max_detection_window:
                                bos_found = True
                                bos_data = bos
                                break
                    
                    if bos_found:
                        # We found a complete supply order block pattern
                        # (bullish doji -> bearish FVG -> bearish BOS)
                        timestamp = doji_candle.timestamp
                        
                        supply_block = OrderBlockDto(
                            type='supply',
                            price_high=doji_candle.close,
                            price_low=doji_candle.open,
                            index=doji_idx,
                            candle=doji_candle,
                            is_doji=True,
                            timestamp=timestamp,
                            doji_data=doji,
                            # Store related data in candle field or extend the DTO to include these fields
                            related_fvg=fvg,
                            bos_data=bos_data,
                            # Add mitigation fields
                            status='active',
                            touched=False,
                            mitigation_percentage=0.0,
                            created_at=datetime.now(timezone.utc).isoformat()
                        )
                        
                        supply_blocks.append(supply_block)
                        processed_fvgs.add(fvg_id)  # Mark this FVG as processed
                        break  # Found a valid pattern, move to next doji
        
        return demand_blocks, supply_blocks
    
    def check_mitigation(self, order_block: OrderBlockDto, candles: List[CandleDto]) -> OrderBlockDto:
        """
        Check if an order block has been mitigated by new candles.
        
        Args:
            order_block: The order block to check
            candles: Candles to check against the order block
            
        Returns:
            Updated order block with mitigation status
        """
        # Order block price range
        ob_high = order_block.price_high
        ob_low = order_block.price_low
        ob_type = order_block.type  # 'demand' or 'supply'
        ob_idx = order_block.index
        
        # Calculate order block zone size
        zone_size = ob_high - ob_low
        if zone_size <= 0:
            order_block.status = 'mitigated'
            order_block.mitigation_percentage = 100.0
            return order_block  # Invalid zone, consider fully mitigated
        
        # Extract mitigation threshold from params
        mitigation_threshold = self.params['mitigation_threshold']
        
        # Continue with existing mitigation percentage if available
        mitigation_percentage = order_block.mitigation_percentage if hasattr(order_block, 'mitigation_percentage') else 0.0
        was_touched = order_block.touched if hasattr(order_block, 'touched') else False
        
        # Track mitigated areas in the zone
        mitigated_areas = []  # List of (start, end) tuples representing mitigated ranges
        
        for candle in candles:
            # Skip candles before or at the order block formation
            if 'index' in candle and candle['index'] <= ob_idx:
                continue
            
            # First, check if this candle interacts with the order block zone
            candle_interacts = False
            
            # For demand (bullish) order blocks
            if ob_type == 'demand':
                # Check if candle trades into the zone
                if ((candle['low'] <= ob_high and candle['low'] >= ob_low) or
                    (candle['high'] >= ob_low and candle['high'] <= ob_high) or
                    (candle['low'] <= ob_low and candle['high'] >= ob_high)):
                    was_touched = True
                    candle_interacts = True
            
            # For supply (bearish) order blocks
            elif ob_type == 'supply':
                # Check if candle trades into the zone
                if ((candle['high'] >= ob_low and candle['high'] <= ob_high) or
                    (candle['low'] <= ob_high and candle['low'] >= ob_low) or
                    (candle['low'] <= ob_low and candle['high'] >= ob_high)):
                    was_touched = True
                    candle_interacts = True
            
            # If this candle interacts with the zone, calculate mitigated area
            if candle_interacts:
                # Calculate the overlap between candle and order block
                overlap_high = min(candle['high'], ob_high)
                overlap_low = max(candle['low'], ob_low)
                
                if overlap_high > overlap_low:
                    # There is an overlap - add it to mitigated areas
                    mitigated_areas.append((overlap_low, overlap_high))
        
        # Calculate total mitigated zone by merging overlapping ranges
        if mitigated_areas:
            # Sort by start of range
            mitigated_areas.sort(key=lambda x: x[0])
            
            # Merge overlapping ranges
            merged_areas = []
            current_start, current_end = mitigated_areas[0]
            
            for start, end in mitigated_areas[1:]:
                if start <= current_end:
                    # Ranges overlap, extend current range
                    current_end = max(current_end, end)
                else:
                    # No overlap, add current range and start a new one
                    merged_areas.append((current_start, current_end))
                    current_start, current_end = start, end
            
            # Add the last range
            merged_areas.append((current_start, current_end))
            
            # Calculate total mitigated size
            total_mitigated = sum(end - start for start, end in merged_areas)
            
            # Calculate percentage of zone mitigated
            mitigation_percentage = (total_mitigated / zone_size) * 100.0
        
        # Update order block with mitigation information
        order_block.touched = was_touched
        order_block.mitigation_percentage = mitigation_percentage
        
        # Check if mitigation threshold exceeded
        if mitigation_percentage >= mitigation_threshold * 100:
            order_block.status = 'mitigated'
        
        return order_block
    
    def process_existing_order_blocks(self, existing_blocks: List[OrderBlockDto], candles: List[CandleDto]) -> Tuple[List[OrderBlockDto], List[OrderBlockDto]]:
        """
        Process existing order blocks for mitigation with new candles.
        
        Args:
            existing_blocks: List of existing order blocks from database
            candles: New candles to check for mitigation
            
        Returns:
            Tuple of (updated_blocks, remaining_active_blocks)
        """
        updated_blocks = []
        remaining_active_blocks = []
        
        for block in existing_blocks:
            # Only process active blocks
            if block.status == 'active':
                # Check for mitigation
                updated_block = self.check_mitigation(block, candles)
                updated_blocks.append(updated_block)
                
                # If still active, add to remaining active blocks
                if updated_block.status == 'active':
                    remaining_active_blocks.append(updated_block)
        
        return updated_blocks, remaining_active_blocks
    
    def _serialize_candle(self, candle: CandleDto) -> Dict[str, Any]:
        """
        Convert a CandleDto to a JSON-serializable dictionary.
        
        Args:
            candle: Candle DTO to serialize
            
        Returns:
            JSON-serializable dictionary
        """
        if candle is None:
            return None
        
        result = {
            "symbol": candle.symbol,
            "exchange": candle.exchange,
            "timeframe": candle.timeframe,
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "is_closed": candle.is_closed
        }
        
        # Handle timestamp (could be datetime or string)
        if hasattr(candle, 'timestamp'):
            if isinstance(candle.timestamp, datetime):
                result["timestamp"] = candle.timestamp.isoformat()
            else:
                result["timestamp"] = candle.timestamp
        
        return result

    def _serialize_dto(self, dto) -> Dict[str, Any]:
        """
        Convert a DTO object to a JSON-serializable dictionary.
        
        Args:
            dto: DTO object to serialize
            
        Returns:
            JSON-serializable dictionary
        """
        if dto is None:
            return None
        
        result = {}
        for key, value in vars(dto).items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif hasattr(value, '__dict__'):
                # Handle nested DTOs
                result[key] = self._serialize_dto(value)
            elif isinstance(value, (int, float, str, bool)) or value is None:
                # Primitive types can be stored directly
                result[key] = value
            elif isinstance(value, CandleDto):
                # Handle candle specially
                result[key] = self._serialize_candle(value)
            else:
                # Skip complex types that can't be directly serialized
                pass
        
        return result

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
            'indicators': [IndicatorType.STRUCTURE_BREAK, IndicatorType.FVG, IndicatorType.DOJI_CANDLE]
        }