from typing import Dict, Any, List
from .base import Indicator
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
                - max_age_candles: Maximum age in candles to consider (default: 20)
        """
        default_params = {
            'min_gap_size': 0.2,  # Minimum gap size as percentage
            'max_age_candles': 20  # Maximum age in candles
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect Fair Value Gaps in the provided candle data
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles with at least 'high', 'low', 'open', 'close', and optional 'timestamp'
                - current_price: Current market price (optional)
                
        Returns:
            Dictionary with detected FVGs:
                - bullish_fvgs: List of bullish FVGs
                - bearish_fvgs: List of bearish FVGs
                - strength: Average strength of all detected FVGs
        """
        candles = data.get('candles', [])
        current_price = data.get('current_price')
        
        # Need at least 3 candles to detect FVGs
        if len(candles) < 3:
            logger.warning("Not enough candles to detect FVGs (minimum 3 required)")
            return {
                'bullish_fvgs': [],
                'bearish_fvgs': [],
                'strength': 0.0
            }
        
        bullish_fvgs = []
        bearish_fvgs = []
        
        # Minimum gap size as decimal
        min_gap_pct = self.params['min_gap_size'] / 100.0
        
        # Maximum age in candles
        max_age = self.params['max_age_candles']
        
        # Detect FVGs by comparing candles
        for i in range(2, len(candles)):
            candle_current = candles[i]
            candle_previous = candles[i-1]
            candle_before_previous = candles[i-2]
            
            # Calculate candle index (negative counting from the end)
            candle_index = i - len(candles)
            
            # Skip if FVG would be too old
            if abs(candle_index) > max_age:
                continue
            
            # Detect bullish FVG (gap up)
            if candle_current['low'] > candle_before_previous['high']:
                # Calculate gap size
                gap_size = candle_current['low'] - candle_before_previous['high']
                
                # Calculate gap size as percentage of price
                gap_pct = gap_size / candle_before_previous['high']
                
                # Skip if gap is too small
                if gap_pct < min_gap_pct:
                    continue
                
                # Calculate gap strength (normalized to 0-1 range)
                # Larger gaps and newer FVGs have higher strength
                gap_strength = min(1.0, gap_pct * 5) * (1.0 - abs(candle_index) / max_age)
                
                # Create bullish FVG
                bullish_fvg = {
                    'type': 'bullish',
                    'top': candle_current['low'],
                    'bottom': candle_before_previous['high'],
                    'size': gap_size,
                    'size_percent': gap_pct * 100,  # Convert to percentage
                    'strength': gap_strength,
                    'candle_index': candle_index,
                    'filled': False,
                    'candle': candle_current.copy()  # Include the candle that formed this FVG
                }
                
                # Add timestamp if available
                if 'timestamp' in candle_current:
                    bullish_fvg['timestamp'] = candle_current['timestamp']
                
                # Check if FVG is already filled based on current price
                if current_price is not None:
                    if current_price >= bullish_fvg['bottom'] and current_price <= bullish_fvg['top']:
                        bullish_fvg['filled'] = True
                
                bullish_fvgs.append(bullish_fvg)
            
            # Detect bearish FVG (gap down)
            elif candle_current['high'] < candle_before_previous['low']:
                # Calculate gap size
                gap_size = candle_before_previous['low'] - candle_current['high']
                
                # Calculate gap size as percentage of price
                gap_pct = gap_size / candle_before_previous['low']
                
                # Skip if gap is too small
                if gap_pct < min_gap_pct:
                    continue
                
                # Calculate gap strength (normalized to 0-1 range)
                # Larger gaps and newer FVGs have higher strength
                gap_strength = min(1.0, gap_pct * 5) * (1.0 - abs(candle_index) / max_age)
                
                # Create bearish FVG
                bearish_fvg = {
                    'type': 'bearish',
                    'top': candle_before_previous['low'],
                    'bottom': candle_current['high'],
                    'size': gap_size,
                    'size_percent': gap_pct * 100,  # Convert to percentage
                    'strength': gap_strength,
                    'candle_index': candle_index,
                    'filled': False,
                    'candle': candle_current.copy()  # Include the candle that formed this FVG
                }
                
                # Add timestamp if available
                if 'timestamp' in candle_current:
                    bearish_fvg['timestamp'] = candle_current['timestamp']
                
                # Check if FVG is already filled based on current price
                if current_price is not None:
                    if current_price >= bearish_fvg['bottom'] and current_price <= bearish_fvg['top']:
                        bearish_fvg['filled'] = True
                
                bearish_fvgs.append(bearish_fvg)
        
        # Filter out FVGs that have been filled by subsequent price action
        self._filter_filled_by_price_action(candles, bullish_fvgs, bearish_fvgs)
        
        # Sort FVGs by strength (strongest first)
        bullish_fvgs = sorted(bullish_fvgs, key=lambda x: x['strength'], reverse=True)
        bearish_fvgs = sorted(bearish_fvgs, key=lambda x: x['strength'], reverse=True)
        
        # Calculate average strength
        all_fvgs = bullish_fvgs + bearish_fvgs
        avg_strength = sum(fvg['strength'] for fvg in all_fvgs) / max(1, len(all_fvgs))
        
        return {
            'bullish_fvgs': bullish_fvgs,
            'bearish_fvgs': bearish_fvgs,
            'strength': avg_strength
        }
    
    def _filter_filled_by_price_action(self, candles: List[Dict[str, Any]], 
                                    bullish_fvgs: List[Dict[str, Any]], 
                                    bearish_fvgs: List[Dict[str, Any]]) -> None:
        """
        Check if FVGs have been filled by subsequent price action
        
        Args:
            candles: List of candles
            bullish_fvgs: List of bullish FVGs to check
            bearish_fvgs: List of bearish FVGs to check
        """
        # Get the latest candle index
        latest_idx = len(candles) - 1
        
        # Check each bullish FVG
        for fvg in bullish_fvgs:
            # Get the candle index where this FVG was formed
            fvg_idx = latest_idx + fvg['candle_index']
            
            # Check candles after FVG formation
            for i in range(fvg_idx + 1, len(candles)):
                # If price traded below the FVG top but above bottom, it's partially filled
                if candles[i]['low'] <= fvg['top'] and candles[i]['high'] >= fvg['bottom']:
                    fvg['filled'] = True
                    break
        
        # Check each bearish FVG
        for fvg in bearish_fvgs:
            # Get the candle index where this FVG was formed
            fvg_idx = latest_idx + fvg['candle_index']
            
            # Check candles after FVG formation
            for i in range(fvg_idx + 1, len(candles)):
                # If price traded above the FVG bottom but below top, it's partially filled
                if candles[i]['high'] >= fvg['bottom'] and candles[i]['low'] <= fvg['top']:
                    fvg['filled'] = True
                    break
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns data requirements for FVG detection
        
        Returns:
            Dictionary with requirements
        """
        return {
            'candles': True,
            'lookback_period': self.params['max_age_candles'] + 2,  # Need extra candles for gap detection
            'timeframes': ['1m', '5m', '15m', '1h', '4h', '1d'],  # Supported timeframes
            'indicators': []  # No dependency on other indicators
        }