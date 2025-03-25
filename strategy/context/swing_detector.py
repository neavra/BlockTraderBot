# strategy/context/swing_detection.py
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class SwingDetector:
    """
    Detects swing highs and lows in price data using various algorithms
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize swing detector with parameters
        
        Args:
            params: Dictionary with detection parameters:
                - lookback: Number of candles to consider for swing detection
                - threshold: Minimum percentage change to consider a swing
                - method: Detection method ('zigzag', 'fractals', 'window')
        """
        default_params = {
            'lookback': 5,          # Number of candles to look back
            'threshold': 0.5,        # Minimum % change required for swing (0.5 = 0.5%)
            'method': 'window',      # Detection method: 'window', 'zigzag', or 'fractals'
            'strength_measure': 'volume',  # How to measure swing strength: 'volume', 'range', or 'time'
            'min_swing_separation': 3  # Minimum candles between swing points
        }
        
        if params:
            default_params.update(params)
        
        self.params = default_params
    
    def detect_swings(self, candles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect swing highs and lows in the candle data
        
        Args:
            candles: List of candle dictionaries with OHLCV data
            
        Returns:
            Dictionary with lists of swing highs and lows
        """
        if len(candles) < self.params['lookback'] * 2 + 1:
            return {'highs': [], 'lows': []}
        
        method = self.params['method'].lower()
        
        if method == 'window':
            return self._detect_swings_window(candles)
        elif method == 'zigzag':
            return self._detect_swings_zigzag(candles)
        elif method == 'fractals':
            return self._detect_swings_fractals(candles)
        else:
            logger.warning(f"Unknown swing detection method: {method}. Using window method.")
            return self._detect_swings_window(candles)
    
    def _detect_swings_window(self, candles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect swings using sliding window algorithm
        
        This method looks for local maxima and minima within a window of candles
        """
        lookback = self.params['lookback']
        threshold_pct = self.params['threshold'] / 100.0  # Convert to decimal
        
        # Extract high and low prices
        highs = [c.get('high', c.get('close', 0)) for c in candles]
        lows = [c.get('low', c.get('close', 0)) for c in candles]
        closes = [c.get('close', 0) for c in candles]
        timestamps = [c.get('timestamp', '') for c in candles]
        volumes = [c.get('volume', 0) for c in candles]
        
        swing_highs = []
        swing_lows = []
        
        # Start from the lookback position so we have enough history
        for i in range(lookback, len(candles) - lookback):
            # Check for swing high
            if all(highs[i] >= highs[i-j] for j in range(1, lookback+1)) and \
               all(highs[i] >= highs[i+j] for j in range(1, lookback+1)):
                
                # Calculate swing strength
                left_min = min(lows[i-lookback:i])
                swing_strength = (highs[i] - left_min) / left_min
                
                # Only record if swing is significant
                if swing_strength >= threshold_pct:
                    # Calculate relative strength based on volume
                    vol_strength = volumes[i] / np.mean(volumes[i-lookback:i+lookback+1]) if volumes[i] else 1.0
                    
                    swing_highs.append({
                        'price': highs[i],
                        'index': i,
                        'timestamp': timestamps[i],
                        'strength': min(1.0, swing_strength * 20),  # Normalize to 0-1 range
                        'volume_strength': min(1.0, vol_strength),
                        'type': 'high'
                    })
            
            # Check for swing low
            if all(lows[i] <= lows[i-j] for j in range(1, lookback+1)) and \
               all(lows[i] <= lows[i+j] for j in range(1, lookback+1)):
                
                # Calculate swing strength
                left_max = max(highs[i-lookback:i])
                swing_strength = (left_max - lows[i]) / left_max
                
                # Only record if swing is significant
                if swing_strength >= threshold_pct:
                    # Calculate relative strength based on volume
                    vol_strength = volumes[i] / np.mean(volumes[i-lookback:i+lookback+1]) if volumes[i] else 1.0
                    
                    swing_lows.append({
                        'price': lows[i],
                        'index': i,
                        'timestamp': timestamps[i],
                        'strength': min(1.0, swing_strength * 20),  # Normalize to 0-1 range
                        'volume_strength': min(1.0, vol_strength),
                        'type': 'low'
                    })
        
        # Remove swings that are too close to each other (keep stronger ones)
        swing_highs = self._filter_close_swings(swing_highs)
        swing_lows = self._filter_close_swings(swing_lows)
        
        return {
            'highs': swing_highs,
            'lows': swing_lows
        }
    
    def _detect_swings_zigzag(self, candles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect swings using a ZigZag algorithm
        
        This implementation uses a threshold-based ZigZag method to find significant swing points
        """
        threshold_pct = self.params['threshold'] / 100.0  # Convert to decimal
        min_separation = self.params['min_swing_separation']
        
        # Extract price data
        highs = [c.get('high', c.get('close', 0)) for c in candles]
        lows = [c.get('low', c.get('close', 0)) for c in candles]
        closes = [c.get('close', 0) for c in candles]
        timestamps = [c.get('timestamp', '') for c in candles]
        volumes = [c.get('volume', 0) for c in candles]
        
        # Find initial point
        swing_dir = 0  # 0=unknown, 1=seeking high, -1=seeking low
        last_high_idx = 0
        last_low_idx = 0
        last_high = highs[0]
        last_low = lows[0]
        
        swing_highs = []
        swing_lows = []
        
        # Process each candle
        for i in range(1, len(candles)):
            if swing_dir == 0:
                # Determine initial direction
                if highs[i] > last_high:
                    last_high = highs[i]
                    last_high_idx = i
                    swing_dir = 1  # Seeking high
                elif lows[i] < last_low:
                    last_low = lows[i]
                    last_low_idx = i
                    swing_dir = -1  # Seeking low
            
            elif swing_dir == 1:  # Seeking high
                if highs[i] > last_high:
                    # New higher high
                    last_high = highs[i]
                    last_high_idx = i
                
                elif lows[i] < last_low or (last_high - lows[i]) / last_high >= threshold_pct:
                    # Significant pullback, record swing high and switch direction
                    
                    # Calculate strength metrics
                    vol_strength = volumes[last_high_idx] / np.mean(volumes[max(0, last_high_idx-5):min(len(volumes), last_high_idx+6)]) if volumes[last_high_idx] else 1.0
                    relative_size = (last_high - last_low) / last_low
                    
                    swing_highs.append({
                        'price': last_high,
                        'index': last_high_idx,
                        'timestamp': timestamps[last_high_idx],
                        'strength': min(1.0, relative_size * 20),  # Normalize to 0-1 range
                        'volume_strength': min(1.0, vol_strength),
                        'type': 'high'
                    })
                    
                    # Reset for seeking low
                    last_low = lows[i]
                    last_low_idx = i
                    swing_dir = -1
            
            elif swing_dir == -1:  # Seeking low
                if lows[i] < last_low:
                    # New lower low
                    last_low = lows[i]
                    last_low_idx = i
                
                elif highs[i] > last_high or (highs[i] - last_low) / last_low >= threshold_pct:
                    # Significant bounce, record swing low and switch direction
                    
                    # Calculate strength metrics
                    vol_strength = volumes[last_low_idx] / np.mean(volumes[max(0, last_low_idx-5):min(len(volumes), last_low_idx+6)]) if volumes[last_low_idx] else 1.0
                    relative_size = (last_high - last_low) / last_high
                    
                    swing_lows.append({
                        'price': last_low,
                        'index': last_low_idx,
                        'timestamp': timestamps[last_low_idx],
                        'strength': min(1.0, relative_size * 20),  # Normalize to 0-1 range
                        'volume_strength': min(1.0, vol_strength),
                        'type': 'low'
                    })
                    
                    # Reset for seeking high
                    last_high = highs[i]
                    last_high_idx = i
                    swing_dir = 1
        
        # Filter out swings that are too close to each other
        swing_highs = self._filter_close_swings(swing_highs)
        swing_lows = self._filter_close_swings(swing_lows)
        
        return {
            'highs': swing_highs,
            'lows': swing_lows
        }
    
    def _detect_swings_fractals(self, candles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect swings using Bill Williams' Fractal indicator
        
        A bearish fractal forms when a high is surrounded by 2 lower highs on each side
        A bullish fractal forms when a low is surrounded by 2 higher lows on each side
        """
        # Default Williams' fractal looks at 5 candles (2 on each side)
        lookback = min(2, self.params.get('lookback', 2))
        threshold_pct = self.params['threshold'] / 100.0  # Convert to decimal
        
        # Extract price data
        highs = [c.get('high', c.get('close', 0)) for c in candles]
        lows = [c.get('low', c.get('close', 0)) for c in candles]
        timestamps = [c.get('timestamp', '') for c in candles]
        volumes = [c.get('volume', 0) for c in candles]
        
        swing_highs = []
        swing_lows = []
        
        # Need at least 2*lookback+1 candles
        if len(candles) < 2*lookback+1:
            return {'highs': [], 'lows': []}
        
        # Find fractal patterns
        for i in range(lookback, len(candles) - lookback):
            # Check for bearish fractal (swing high)
            if all(highs[i] > highs[i-j] for j in range(1, lookback+1)) and \
               all(highs[i] > highs[i+j] for j in range(1, lookback+1)):
                
                # Calculate significance
                left_min = min(lows[i-lookback:i])
                swing_strength = (highs[i] - left_min) / left_min
                
                # Only record if significant
                if swing_strength >= threshold_pct:
                    # Calculate volume strength
                    vol_strength = volumes[i] / np.mean(volumes[max(0, i-5):min(len(volumes), i+6)]) if volumes[i] else 1.0
                    
                    swing_highs.append({
                        'price': highs[i],
                        'index': i,
                        'timestamp': timestamps[i],
                        'strength': min(1.0, swing_strength * 20),  # Normalize to 0-1 range
                        'volume_strength': min(1.0, vol_strength),
                        'type': 'high'
                    })
            
            # Check for bullish fractal (swing low)
            if all(lows[i] < lows[i-j] for j in range(1, lookback+1)) and \
               all(lows[i] < lows[i+j] for j in range(1, lookback+1)):
                
                # Calculate significance
                left_max = max(highs[i-lookback:i])
                swing_strength = (left_max - lows[i]) / left_max
                
                # Only record if significant
                if swing_strength >= threshold_pct:
                    # Calculate volume strength
                    vol_strength = volumes[i] / np.mean(volumes[max(0, i-5):min(len(volumes), i+6)]) if volumes[i] else 1.0
                    
                    swing_lows.append({
                        'price': lows[i],
                        'index': i,
                        'timestamp': timestamps[i],
                        'strength': min(1.0, swing_strength * 20),  # Normalize to 0-1 range
                        'volume_strength': min(1.0, vol_strength),
                        'type': 'low'
                    })
        
        # Filter out swings that are too close to each other
        swing_highs = self._filter_close_swings(swing_highs)
        swing_lows = self._filter_close_swings(swing_lows)
        
        return {
            'highs': swing_highs,
            'lows': swing_lows
        }
    
    def _filter_close_swings(self, swings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter out swing points that are too close to each other
        
        When two swing points are too close, keep the one with higher strength
        """
        if not swings:
            return []
        
        min_separation = self.params['min_swing_separation']
        
        # Sort by index
        sorted_swings = sorted(swings, key=lambda x: x['index'])
        
        # Filter swings that are too close
        filtered_swings = [sorted_swings[0]]
        
        for i in range(1, len(sorted_swings)):
            curr_swing = sorted_swings[i]
            last_swing = filtered_swings[-1]
            
            if curr_swing['index'] - last_swing['index'] >= min_separation:
                # Enough separation, keep this swing
                filtered_swings.append(curr_swing)
            else:
                # Too close, keep the stronger one
                if curr_swing['strength'] > last_swing['strength']:
                    # Replace the last one with this one
                    filtered_swings[-1] = curr_swing
        
        return filtered_swings
    
    def find_significant_levels(self, candles: List[Dict[str, Any]], 
                              swings: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[float]]:
        """
        Find significant support/resistance levels based on swing points
        
        Args:
            candles: List of candle dictionaries
            swings: Dictionary with swing highs and lows
            
        Returns:
            Dictionary with support and resistance levels
        """
        highs = swings.get('highs', [])
        lows = swings.get('lows', [])
        
        # Extract prices
        high_prices = [h['price'] for h in highs]
        low_prices = [l['price'] for l in lows]
        
        # Find clusters of prices (simplified approach)
        resistance_levels = self._cluster_prices(high_prices)
        support_levels = self._cluster_prices(low_prices)
        
        return {
            'resistance': resistance_levels,
            'support': support_levels
        }
    
    def _cluster_prices(self, prices: List[float], tolerance: float = 0.01) -> List[float]:
        """
        Cluster nearby price levels
        
        Args:
            prices: List of price levels
            tolerance: Relative distance for clustering (percentage)
            
        Returns:
            List of clustered price levels
        """
        if not prices:
            return []
        
        # Sort prices
        sorted_prices = sorted(prices)
        
        # Initialize clusters
        clusters = []
        current_cluster = [sorted_prices[0]]
        
        # Group prices into clusters
        for i in range(1, len(sorted_prices)):
            current_price = sorted_prices[i]
            cluster_avg = sum(current_cluster) / len(current_cluster)
            
            # Check if price belongs to current cluster
            if abs(current_price - cluster_avg) / cluster_avg <= tolerance:
                current_cluster.append(current_price)
            else:
                # Start a new cluster
                clusters.append(current_cluster)
                current_cluster = [current_price]
        
        # Add the last cluster
        clusters.append(current_cluster)
        
        # Calculate average price for each cluster
        return [sum(cluster) / len(cluster) for cluster in clusters]