from typing import Dict, Any, List
from datetime import datetime, timezone
from .base import Indicator
from strategy.domain.dto.fvg_dto import FvgDto, FvgResultDto
from data.database.repository.fvg_repository import FvgRepository

from shared.domain.dto.candle_dto import CandleDto
import logging

logger = logging.getLogger(__name__)

class FVGIndicator(Indicator):
    """
    Indicator that detects Fair Value Gaps (FVGs) in price action.
    
    A Fair Value Gap occurs when a candle's body completely skips a price range,
    leaving an imbalance in the market. These gaps often represent areas where price
    may return to "fill the gap" in the future.
    
    - Bullish FVG: Low of a candle is above the high of the candle two positions back
    - Bearish FVG: High of a candle is below the low of the candle two positions back
    """
    
    def __init__(self, repository: FvgRepository, params: Dict[str, Any] = None):
        """
        Initialize FVG detector with parameters
        
        Args:
            params: Dictionary containing parameters:
                - min_gap_size: Minimum gap size as percentage (default: 0.2%)
        """

        self.repository = repository

        default_params = {
            'min_gap_size': 0.2,  # Minimum gap size as percentage
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(repository, default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> FvgResultDto:
        """
        Detect Fair Value Gaps in the provided candle data and save them to the database.
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles with at least 'high', 'low', 'open', 'close', and optional 'timestamp'
                - symbol: Trading symbol
                - timeframe: Timeframe
                - exchange: Exchange name
                - current_price: Current market price (optional)
                    
        Returns:
            FvgResultDto with detected bullish and bearish FVGs
        """
        
        # Extract market data information
        candles: List[CandleDto] = data.get("candles")
        symbol = data.get("symbol")
        timeframe = data.get("timeframe")
        exchange = data.get("exchange", "default")
        
        # Need at least 3 candles to detect FVGs
        if len(candles) < 3:
            logger.warning("Not enough candles to detect FVGs (minimum 3 required)")
            return FvgResultDto(
                timestamp=datetime.now(timezone.utc),
                indicator_name="FVG",
                bullish_fvgs=[],
                bearish_fvgs=[]
            )
        
        bullish_fvgs = []
        bearish_fvgs = []
        
        # Minimum gap size as decimal
        min_gap_pct = self.params['min_gap_size'] / 100.0
        
        # Detect FVGs by comparing candles
        # Check high of i-2 and low of i (for bullish)
        # Check low of i-2 and high of i (for bearish)
        for i in range(2, len(candles)):
            candle_current = candles[i]
            candle_before_previous = candles[i-2]
            
            # Calculate candle index, i-1 would be the candle body with the FVG
            candle_index = i - 1
            
            # Detect bullish FVG (gap up)
            if candle_current.low > candle_before_previous.high:
                # Calculate gap size
                gap_size = candle_current.low - candle_before_previous.high
                
                # Calculate gap size as percentage of price
                gap_pct = gap_size / candle_before_previous.high
                
                # Skip if gap is too small
                if gap_pct < min_gap_pct:
                    continue
                
                # Create bullish FVG DTO
                timestamp = candle_current.timestamp
                bullish_fvg = FvgDto(
                    type="bullish",
                    top=candle_current.low,
                    bottom=candle_before_previous.high,
                    size=gap_size,
                    size_percent=gap_pct * 100,  # Convert to percentage
                    candle_index=candle_index,
                    filled=False,
                    timestamp=timestamp,
                    candle=candle_current
                )
                
                bullish_fvgs.append(bullish_fvg)
            
            # Detect bearish FVG (gap down)
            elif candle_current.high < candle_before_previous.low:
                # Calculate gap size
                gap_size = candle_before_previous.low - candle_current.high
                
                # Calculate gap size as percentage of price
                gap_pct = gap_size / candle_before_previous.low
                
                # Skip if gap is too small
                if gap_pct < min_gap_pct:
                    continue
                
                # Create bearish FVG DTO
                timestamp = candle_current.timestamp
                bearish_fvg = FvgDto(
                    type="bearish",
                    top=candle_before_previous.low,
                    bottom=candle_current.high,
                    size=gap_size,
                    size_percent=gap_pct * 100,  # Convert to percentage
                    candle_index=candle_index,
                    filled=False,
                    timestamp=timestamp,
                    candle=candle_current
                )
                
                bearish_fvgs.append(bearish_fvg)
        
        # Filter out FVGs that have been filled by subsequent price action
        self._filter_filled_by_price_action(candles, bullish_fvgs, bearish_fvgs)
        
        # Save detected FVGs to the database
        try:
            # Prepare FVGs for database insertion
            fvgs_data = []
            
            # Process bullish FVGs
            for fvg in bullish_fvgs + bearish_fvgs:
                # Convert FVG to database format
                fvg_data = {
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "type": fvg.type,
                    "top": float(fvg.top),
                    "bottom": float(fvg.bottom),
                    "size": float(fvg.size),
                    "size_percent": float(fvg.size_percent),
                    "filled": fvg.filled,
                    "candle_index": fvg.candle_index,
                    "timestamp": fvg.timestamp if isinstance(fvg.timestamp, datetime) else datetime.fromisoformat(fvg.timestamp),
                    
                    # Store the JSON-serializable version of the candle data
                    "candle_data": self._serialize_candle(fvg.candle),
                    
                    # Required fields from your model
                    "indicator_id": 2,  # Assuming 2 is the ID for FVG indicator in the indicators table
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                }
                
                fvgs_data.append(fvg_data)
            
            # Bulk insert the FVGs
            if fvgs_data:
                created_fvgs = await self.repository.bulk_create_fvgs(fvgs_data)
                logger.info(f"Saved {len(created_fvgs)} FVGs to database")
                
        except Exception as e:
            logger.error(f"Error saving FVGs to database: {str(e)}")
        
        # Create and return FVG result DTO
        return FvgResultDto(
            timestamp=datetime.now(timezone.utc),
            indicator_name="FVG",
            bullish_fvgs=bullish_fvgs,
            bearish_fvgs=bearish_fvgs
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
    
    def _filter_filled_by_price_action(self, candles: List[CandleDto], 
                                    bullish_fvgs: List[FvgDto], 
                                    bearish_fvgs: List[FvgDto]) -> None:
        """
        Check if FVGs have been filled by subsequent price action
        
        Args:
            candles: List of candles
            bullish_fvgs: List of bullish FVGs to check
            bearish_fvgs: List of bearish FVGs to check
        """
        
        # Check each bullish FVG
        for fvg in bullish_fvgs:
            # Get the candle index where this FVG was formed
            fvg_idx = fvg.candle_index
            
            # Check candles after FVG formation, ignore the candle right after the FVG as it forms part of the FVG
            for i in range(fvg_idx + 2, len(candles)):
                # If price traded below the FVG top but above bottom, it's partially filled
                if candles[i].low <= fvg.top:
                    fvg.filled = True
                    break
        
        # Check each bearish FVG
        for fvg in bearish_fvgs:
            # Get the candle index where this FVG was formed
            fvg_idx = fvg.candle_index
            
            # Check candles after FVG formation, ignore the candle right after the FVG as it forms part of the FVG
            for i in range(fvg_idx + 2, len(candles)):
                # If price traded above the FVG bottom but below top, it's partially filled
                if candles[i].high >= fvg.bottom:
                    fvg.filled = True
                    break
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns data requirements for FVG detection
        
        Returns:
            Dictionary with requirements
        """
        return {
            'candles': True,
            'lookback_period': 30,  # Need extra candles for gap detection
            'timeframes': ['1m', '5m', '15m', '1h', '4h', '1d'],  # Supported timeframes
            'indicators': []  # No dependency on other indicators
        }