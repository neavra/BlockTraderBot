from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from .base import Indicator
from strategy.domain.dto.bos_dto import StructureBreakDto, StructureBreakResultDto
from strategy.domain.models.market_context import MarketContext
from data.database.repository.bos_repository import BosRepository

from shared.domain.dto.candle_dto import CandleDto
import logging

logger = logging.getLogger(__name__)

class StructureBreakIndicator(Indicator):
    """
    Indicator that detects Breaking of Structure (BOS) events.
    
    A Breaking of Structure (BOS) occurs when price breaks above recent swing highs
    or below recent swing lows, indicating a potential trend continuation or reversal.
    
    This indicator detects four types of structure breaks:
    1. Higher High (HH): Price breaks above the recent swing high
    2. Lower Low (LL): Price breaks below the recent swing low
    3. Higher Low (HL): Price creates a low that's higher than the previous swing low
    4. Lower High (LH): Price creates a high that's lower than the previous swing high
    """
    
    def __init__(self, repository: BosRepository, params: Dict[str, Any] = None):
        """
        Initialize Breaking of Structure detector with parameters
        
        Args:
            params: Dictionary containing parameters:
                - lookback_period: Number of candles to analyze
                - confirmation_candles: Number of candles to confirm break (default: 1)
                - min_break_percentage: Minimum percentage break beyond structure (default: 0.05%)
        """
        self.repository = repository

        default_params = {
            'lookback_period': 10,         # Number of candles to look back
            'confirmation_candles': 1,      # Number of candles to confirm the break
            'min_break_percentage': 0.0005  # Minimum 0.05% break beyond structure
        }
        
        
        if params:
            default_params.update(params)
            
        super().__init__(repository, default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> StructureBreakResultDto:
        """
        Detect Breaking of Structure events in the provided data and save them to the database.
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles
                - market_contexts: Market context objects with swing high/low information
                - symbol: Trading symbol
                - timeframe: Timeframe
                - exchange: Exchange name
                
        Returns:
            StructureBreakResultDto with detected structure breaks
        """
        candles: List[CandleDto] = data.get("candles")
        market_contexts = data.get("market_contexts")
        
        # Extract market data information
        symbol = data.get("symbol")
        timeframe = data.get("timeframe")
        exchange = data.get("exchange", "default")
        
        # Check if we have at least one market context
        if not market_contexts or len(market_contexts) == 0:
            logger.warning("No market contexts provided, cannot detect structure breaks")
            return self._get_empty_result()
            
        market_context: MarketContext = market_contexts[0]
        
        # Need enough candles to analyze
        if len(candles) < 3:
            logger.warning("Not enough candles to detect structure breaks (minimum 3 required)")
            return self._get_empty_result()
        
        # Need market context with swing points to detect breaks
        if not market_context or not market_context.swing_high or not market_context.swing_low:
            logger.warning("No market context provided, cannot detect structure breaks")
            return self._get_empty_result()
        
        # Get recent swing points from market context
        swing_high = market_context.swing_high
        swing_low = market_context.swing_low
        
        # Check if swing points are available
        if not swing_high or not swing_low:
            logger.info("No swing points available in market context")
            return self._get_empty_result()
        
        # Extract swing values from market context (accounting for potential dict or object structure)
        swing_high_price = swing_high.get('price') if isinstance(swing_high, dict) else getattr(swing_high, 'price', None)
        swing_low_price = swing_low.get('price') if isinstance(swing_low, dict) else getattr(swing_low, 'price', None)
        
        if swing_high_price is None or swing_low_price is None:
            logger.warning("Invalid swing point data in market context")
            return self._get_empty_result()
        
        # Now let's detect the structure breaks
        bullish_breaks = []  # Higher highs and higher lows
        bearish_breaks = []  # Lower lows and lower highs
        
        # Calculate minimum break thresholds based on swing points rather than current price
        min_break_high = swing_high_price * self.params['min_break_percentage']
        min_break_low = swing_low_price * self.params['min_break_percentage']
        
        # Process each candle in the lookback period (we need to consider relative position to swings)
        lookback_period = min(self.params['lookback_period'], len(candles))
        
        for i in range(1, lookback_period + 1):
            candle_idx = len(candles) - i
            if candle_idx < 0:
                break
                
            candle = candles[candle_idx]
            timestamp = candle.timestamp
            
            # Higher High detection (bullish)
            if candle.high > swing_high_price + min_break_high:
                # Check if confirmed by N candles staying above
                if self._is_break_confirmed(candles, candle_idx, 'high', swing_high_price):
                    bullish_breaks.append(StructureBreakDto(
                        index=candle_idx,
                        break_type='higher_high',
                        break_value=candle.high - swing_high_price,
                        break_percentage=(candle.high - swing_high_price) / swing_high_price,
                        swing_reference=swing_high_price,
                        candle=candle,
                        timestamp=timestamp,
                    ))
            
            # Lower Low detection (bearish)
            if candle.low < swing_low_price - min_break_low:
                # Check if confirmed by N candles staying below
                if self._is_break_confirmed(candles, candle_idx, 'low', swing_low_price):
                    bearish_breaks.append(StructureBreakDto(
                        index=candle_idx,
                        break_type='lower_low',
                        break_value=swing_low_price - candle.low,
                        break_percentage=(swing_low_price - candle.low) / swing_low_price,
                        swing_reference=swing_low_price,
                        candle=candle,
                        timestamp=timestamp,
                    ))
            
            # Higher Low detection (bullish)
            # Need to have a previous swing low and current low should be higher
            if candle.low > swing_low_price + min_break_low:
                # No confirmation needed for HL/LH since they're not actual "breaks"
                bullish_breaks.append(StructureBreakDto(
                    index=candle_idx,
                    break_type='higher_low',
                    break_value=candle.low - swing_low_price,
                    break_percentage=(candle.low - swing_low_price) / swing_low_price,
                    swing_reference=swing_low_price,
                    candle=candle,
                    timestamp=timestamp,
                ))
            
            # Lower High detection (bearish)
            # Need to have a previous swing high and current high should be lower
            if candle.high < swing_high_price - min_break_high:
                bearish_breaks.append(StructureBreakDto(
                    index=candle_idx,
                    break_type='lower_high',
                    break_value=swing_high_price - candle.high,
                    break_percentage=(swing_high_price - candle.high) / swing_high_price,
                    swing_reference=swing_high_price,
                    candle=candle,
                    timestamp=timestamp,
                ))
        
        # Sort each list by index (most recent first)
        bullish_breaks.sort(key=lambda x: x.index, reverse=True)
        bearish_breaks.sort(key=lambda x: x.index, reverse=True)
        
        # Save the detected structure breaks to the database
        try:
            all_breaks = bullish_breaks + bearish_breaks
            if all_breaks:
                # Prepare the database records
                bos_records = []
                for bos in all_breaks:
                    # Create the database record
                    bos_data = {
                        "exchange": exchange,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "break_type": bos.break_type,
                        "break_value": float(bos.break_value),
                        "break_percentage": float(bos.break_percentage) * 100,  # Convert to percentage
                        "swing_reference": float(bos.swing_reference),
                        "confirmed": True,  # We've already confirmed it
                        "confirmation_candles": self.params['confirmation_candles'],
                        "candle_index": bos.index,
                        "timestamp": bos.timestamp if isinstance(bos.timestamp, datetime) else 
                                    datetime.fromisoformat(bos.timestamp.replace('Z', '+00:00')),
                        "candle_data": self._serialize_candle(bos.candle),
                        "strength": 0.8,  # Default strength value
                        
                        # Add indicator ID (use the correct enum value in your implementation)
                        "indicator_id": 3,  # Assuming 3 is the ID for STRUCTURE_BREAK in your indicator registry
                        
                        # Add timestamps
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc)
                    }
                    
                    bos_records.append(bos_data)
                
                # Bulk create the records
                if bos_records:
                    created_records = await self.repository.bulk_create_bos(bos_records)
                    logger.info(f"Saved {len(created_records)} structure breaks to database")
        except Exception as e:
            logger.error(f"Error saving structure breaks to database: {e}")
        
        # Create and return result DTO
        return StructureBreakResultDto(
            timestamp=datetime.now(timezone.utc),
            indicator_name="StructureBreak",
            bullish_breaks=bullish_breaks,
            bearish_breaks=bearish_breaks
        )
    
    async def process_existing_indicators(self, indicators: List[Any], candles: List[CandleDto]):
        return None

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
    
    def _is_break_confirmed(self, candles: List[CandleDto], break_idx: int, 
                            price_type: str, reference_price: float) -> bool:
        """
        Check if a structure break is confirmed by subsequent candles
        
        Args:
            candles: List of candle data
            break_idx: Index of the candle that broke structure
            price_type: 'high' for higher high breaks, 'low' for lower low breaks
            reference_price: The price level that was broken
            
        Returns:
            True if confirmed, False otherwise
        """
        confirmation_needed = self.params['confirmation_candles']
        
        # If no confirmation needed
        if confirmation_needed <= 0:
            return True
        
        # Count confirmed candles
        confirmed_count = 0
        
        # Check candles after the break
        for i in range(break_idx + 1, len(candles)):
            # Break the loop if we've found enough confirmations
            if confirmed_count >= confirmation_needed:
                break
                
            # For higher high breaks, check if highs remain above the reference
            if price_type == 'high' and candles[i].high > reference_price:
                confirmed_count += 1
            # For lower low breaks, check if lows remain below the reference
            elif price_type == 'low' and candles[i].low < reference_price:
                confirmed_count += 1
            else:
                # Break confirmed streak
                break
        
        # Return true if we have enough confirmations
        return confirmed_count >= confirmation_needed
    
    def _get_empty_result(self) -> StructureBreakResultDto:
        """Return an empty result structure when no breaks can be detected"""
        return StructureBreakResultDto(
            timestamp=datetime.now(timezone.utc),
            indicator_name="StructureBreak",
            bullish_breaks=[],
            bearish_breaks=[]
        )
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns data requirements for structure break detection
        
        Returns:
            Dictionary with requirements
        """
        return {
            'candles': True,
            'market_context': True,  # Requires market context with swing points
            'lookback_period': 50,
            'timeframes': ['1m', '5m', '15m', '30m', '1h', '4h', '1d'],  # Supported timeframes
            'indicators': []  # No dependency on other indicators
        }