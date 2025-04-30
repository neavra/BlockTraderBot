from typing import Dict, Any, List
from datetime import datetime, timezone
from .base import Indicator
from strategy.domain.dto.fvg_dto import FvgDto, FvgResultDto
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
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize FVG detector with parameters
        
        Args:
            params: Dictionary containing parameters:
                - min_gap_size: Minimum gap size as percentage (default: 0.2%)
        """
        default_params = {
            'min_gap_size': 0.2,  # Minimum gap size as percentage
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(default_params)
    
    async def calculate(self, data: Dict[str,Any]) -> FvgResultDto:
        """
        Detect Fair Value Gaps in the provided candle data
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles with at least 'high', 'low', 'open', 'close', and optional 'timestamp'
                - current_price: Current market price (optional)
                
        Returns:
            FvgResultDto with detected bullish and bearish FVGs
        """
        
        # Need at least 3 candles to detect FVGs
        candles: List[CandleDto] = data.get("candles")
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
                gap_size = candle_before_previous.low - candle_current.low
                
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
        
        # Create and return FVG result DTO
        return FvgResultDto(
            timestamp=datetime.now(timezone.utc),
            indicator_name="FVG",
            bullish_fvgs=bullish_fvgs,
            bearish_fvgs=bearish_fvgs
        )
    
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