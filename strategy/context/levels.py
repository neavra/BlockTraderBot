# strategy/context/levels.py
import numpy as np
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SupportResistanceDetector:
    """
    Detects support and resistance levels using various methods
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        Initialize support/resistance detector with parameters
        
        Args:
            params: Dictionary with detection parameters:
                - method: Detection method ('swing', 'histogram', 'fibonacci')
                - window_size: Window size for price histogram
                - num_levels: Maximum number of levels to detect
                - threshold: Minimum strength threshold for level detection
        """
        default_params = {
            'method': 'swing',         # 'swing', 'histogram', 'fibonacci', 'all'
            'window_size': 100,        # Window size for histogram method
            'num_levels': 5,           # Maximum number of levels to detect
            'threshold': 0.3,          # Minimum strength threshold (0-1)
            'zone_size': 0.2,          # Zone size as percentage of price
            'buffer': 0.1,             # Buffer around levels (%)
        }
        
        if params:
            default_params.update(params)
        
        self.params = default_params
    
    def detect_levels(self, candles: List[Dict[str, Any]], 
                    swings: Dict[str, List[Dict[str, Any]]] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect support and resistance levels
        
        Args:
            candles: List of candle dictionaries with OHLCV data
            swings: Dictionary with swing highs and lows (optional)
            
        Returns:
            Dictionary with support and resistance levels
        """
        method = self.params['method'].lower()
        
        # Initialize empty results
        levels = {
            'support': [],
            'resistance': []
        }
        
        # Apply selected method(s)
        if method == 'swing' or method == 'all':
            if swings:
                swing_levels = self._swing_levels(candles, swings)
                levels['support'].extend(swing_levels['support'])
                levels['resistance'].extend(swing_levels['resistance'])
            else:
                logger.warning("Swing method selected but no swing points provided")
        
        if method == 'histogram' or method == 'all':
            histogram_levels = self._histogram_levels(candles)
            levels['support'].extend(histogram_levels['support'])
            levels['resistance'].extend(histogram_levels['resistance'])
        
        if method == 'fibonacci' or method == 'all':
            fibonacci_levels = self._fibonacci_levels(candles, swings)
            levels['support'].extend(fibonacci_levels['support'])
            levels['resistance'].extend(fibonacci_levels['resistance'])
        
        # Sort and limit the number of levels
        levels['support'] = self._consolidate_levels(
            sorted(levels['support'], key=lambda x: x['strength'], reverse=True)
        )
        levels['resistance'] = self._consolidate_levels(
            sorted(levels['resistance'], key=lambda x: x['strength'], reverse=True)
        )
        
        # Limit to the strongest levels
        max_levels = self.params['num_levels']
        levels['support'] = levels['support'][:max_levels]
        levels['resistance'] = levels['resistance'][:max_levels]
        
        return levels
    
    def _swing_levels(self, candles: List[Dict[str, Any]], 
                    swings: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect support and resistance levels based on swing points
        
        Args:
            candles: List of candle dictionaries
            swings: Dictionary with swing highs and lows
            
        Returns:
            Dictionary with support and resistance levels
        """
        # Extract swing points
        highs = swings.get('highs', [])
        lows = swings.get('lows', [])
        
        # Current price
        current_price = candles[-1]['close'] if candles else 0
        
        # Initialize levels
        support_levels = []
        resistance_levels = []
        
        # Process swing highs for resistance
        for high in highs:
            # Skip if too close to current price
            if high['price'] < current_price * (1 + self.params['buffer'] / 100):
                continue
                
            # Check if this level already exists
            if not self._level_exists(resistance_levels, high['price']):
                resistance_levels.append({
                    'price': high['price'],
                    'strength': high.get('strength', 0.5),  # Default strength if not provided
                    'source': 'swing_high',
                    'timestamp': high.get('timestamp', ''),
                    'touches': 1,
                    'zone': [
                        high['price'] * (1 - self.params['zone_size'] / 100),
                        high['price'] * (1 + self.params['zone_size'] / 100)
                    ]
                })
            else:
                # Increment touch count and update strength
                for level in resistance_levels:
                    if self._prices_equal(level['price'], high['price']):
                        level['touches'] += 1
                        level['strength'] = min(1.0, level['strength'] + 0.1)  # Increment strength with each touch
        
        # Process swing lows for support
        for low in lows:
            # Skip if too close to current price
            if low['price'] > current_price * (1 - self.params['buffer'] / 100):
                continue
                
            # Check if this level already exists
            if not self._level_exists(support_levels, low['price']):
                support_levels.append({
                    'price': low['price'],
                    'strength': low.get('strength', 0.5),  # Default strength if not provided
                    'source': 'swing_low',
                    'timestamp': low.get('timestamp', ''),
                    'touches': 1,
                    'zone': [
                        low['price'] * (1 - self.params['zone_size'] / 100),
                        low['price'] * (1 + self.params['zone_size'] / 100)
                    ]
                })
            else:
                # Increment touch count and update strength
                for level in support_levels:
                    if self._prices_equal(level['price'], low['price']):
                        level['touches'] += 1
                        level['strength'] = min(1.0, level['strength'] + 0.1)  # Increment strength with each touch
        
        return {
            'support': support_levels,
            'resistance': resistance_levels
        }
    
    def _histogram_levels(self, candles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect support and resistance levels using price histogram method
        
        This method creates a histogram of price values and identifies high-frequency areas
        
        Args:
            candles: List of candle dictionaries
            
        Returns:
            Dictionary with support and resistance levels
        """
        if not candles:
            return {'support': [], 'resistance': []}
        
        # Extract price data - use highs, lows and closes for better coverage
        highs = np.array([c.get('high', c.get('close', 0)) for c in candles])
        lows = np.array([c.get('low', c.get('close', 0)) for c in candles])
        closes = np.array([c.get('close', 0) for c in candles])
        
        # Combine all price points
        all_prices = np.concatenate([highs, lows, closes])
        
        # Calculate price range
        price_min = np.min(all_prices)
        price_max = np.max(all_prices)
        price_range = price_max - price_min
        
        # Generate bins for histogram
        n_bins = min(len(candles) // 5, 100)  # Reasonable number of bins
        
        if n_bins <= 1:
            return {'support': [], 'resistance': []}
        
        # Create histogram
        hist, bin_edges = np.histogram(all_prices, bins=n_bins)
        
        # Normalize histogram values
        hist_normalized = hist / np.max(hist)
        
        # Find local maxima in histogram
        peak_indices = []
        for i in range(1, len(hist)-1):
            if hist[i] > hist[i-1] and hist[i] > hist[i+1] and hist_normalized[i] > self.params['threshold']:
                peak_indices.append(i)
        
        # Calculate price levels from bin indices
        levels = []
        for idx in peak_indices:
            price_level = (bin_edges[idx] + bin_edges[idx+1]) / 2
            levels.append({
                'price': price_level,
                'strength': hist_normalized[idx],
                'source': 'histogram',
                'touches': hist[idx],
                'zone': [
                    price_level * (1 - self.params['zone_size'] / 100),
                    price_level * (1 + self.params['zone_size'] / 100)
                ]
            })
        
        # Current price
        current_price = candles[-1]['close'] if candles else 0
        
        # Separate into support and resistance based on current price
        support_levels = [level for level in levels if level['price'] < current_price]
        resistance_levels = [level for level in levels if level['price'] > current_price]
        
        return {
            'support': support_levels,
            'resistance': resistance_levels
        }
    
    def _fibonacci_levels(self, candles: List[Dict[str, Any]], 
                        swings: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate Fibonacci retracement and extension levels
        
        Args:
            candles: List of candle dictionaries
            swings: Dictionary with swing highs and lows (optional)
            
        Returns:
            Dictionary with Fibonacci support and resistance levels
        """
        if not candles or len(candles) < 2:
            return {'support': [], 'resistance': []}
        
        # Get significant high and low points
        if swings and swings.get('highs') and swings.get('lows'):
            # Use the most significant swing points
            highs = sorted(swings['highs'], key=lambda x: x.get('strength', 0), reverse=True)
            lows = sorted(swings['lows'], key=lambda x: x.get('strength', 0), reverse=True)
            
            if highs and lows:
                high_price = highs[0]['price']
                low_price = lows[0]['price']
            else:
                return {'support': [], 'resistance': []}
        else:
            # Use price range from candles
            high_price = max(c.get('high', c.get('close', 0)) for c in candles)
            low_price = min(c.get('low', c.get('close', 0)) for c in candles)
        
        # Price range
        price_range = high_price - low_price
        
        # Current price
        current_price = candles[-1]['close'] if candles else 0
        
        # Fibonacci retracement levels
        fib_levels = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
        
        # Fibonacci extension levels
        fib_extensions = [1.272, 1.414, 1.618, 2.0, 2.618]
        
        # Current trend direction
        uptrend = high_price > low_price
        
        support_levels = []
        resistance_levels = []
        
        # Calculate levels
        if uptrend:
            # In uptrend, retracements are potential support, extensions are potential resistance
            for level in fib_levels:
                fib_price = high_price - (price_range * level)
                
                # Skip levels too close to current price
                if abs(fib_price - current_price) / current_price < self.params['buffer'] / 100:
                    continue
                
                if fib_price < current_price:
                    support_levels.append({
                        'price': fib_price,
                        'strength': 0.3 + 0.4 * (1 - level),  # Higher strength for key levels
                        'source': f'fib_retracement_{level}',
                        'touches': 0,
                        'zone': [
                            fib_price * (1 - self.params['zone_size'] / 100),
                            fib_price * (1 + self.params['zone_size'] / 100)
                        ]
                    })
                else:
                    resistance_levels.append({
                        'price': fib_price,
                        'strength': 0.3 + 0.4 * (1 - level),  # Higher strength for key levels
                        'source': f'fib_retracement_{level}',
                        'touches': 0,
                        'zone': [
                            fib_price * (1 - self.params['zone_size'] / 100),
                            fib_price * (1 + self.params['zone_size'] / 100)
                        ]
                    })
            
            # Add extension levels as resistance
            for ext in fib_extensions:
                ext_price = low_price + (price_range * ext)
                
                # Skip levels too close to current price
                if abs(ext_price - current_price) / current_price < self.params['buffer'] / 100:
                    continue
                
                resistance_levels.append({
                    'price': ext_price,
                    'strength': 0.3 + 0.2 * (3 - min(ext, 3)),  # Higher strength for key levels
                    'source': f'fib_extension_{ext}',
                    'touches': 0,
                    'zone': [
                        ext_price * (1 - self.params['zone_size'] / 100),
                        ext_price * (1 + self.params['zone_size'] / 100)
                    ]
                })
        else:
            # In downtrend, retracements are potential resistance, extensions are potential support
            for level in fib_levels:
                fib_price = low_price + (price_range * level)
                
                # Skip levels too close to current price
                if abs(fib_price - current_price) / current_price < self.params['buffer'] / 100:
                    continue
                
                if fib_price > current_price:
                    resistance_levels.append({
                        'price': fib_price,
                        'strength': 0.3 + 0.4 * (1 - level),  # Higher strength for key levels
                        'source': f'fib_retracement_{level}',
                        'touches': 0,
                        'zone': [
                            fib_price * (1 - self.params['zone_size'] / 100),
                            fib_price * (1 + self.params['zone_size'] / 100)
                        ]
                    })
                else:
                    support_levels.append({
                        'price': fib_price,
                        'strength': 0.3 + 0.4 * (1 - level),  # Higher strength for key levels
                        'source': f'fib_retracement_{level}',
                        'touches': 0,
                        'zone': [
                            fib_price * (1 - self.params['zone_size'] / 100),
                            fib_price * (1 + self.params['zone_size'] / 100)
                        ]
                    })
            
            # Add extension levels as support
            for ext in fib_extensions:
                ext_price = high_price - (price_range * ext)
                
                # Skip levels too close to current price
                if abs(ext_price - current_price) / current_price < self.params['buffer'] / 100:
                    continue
                
                support_levels.append({
                    'price': ext_price,
                    'strength': 0.3 + 0.2 * (3 - min(ext, 3)),  # Higher strength for key levels
                    'source': f'fib_extension_{ext}',
                    'touches': 0,
                    'zone': [
                        ext_price * (1 - self.params['zone_size'] / 100),
                        ext_price * (1 + self.params['zone_size'] / 100)
                    ]
                })
        
        return {
            'support': support_levels,
            'resistance': resistance_levels
        }
    
    def _level_exists(self, levels: List[Dict[str, Any]], price: float) -> bool:
        """
        Check if a price level already exists in the list
        
        Args:
            levels: List of level dictionaries
            price: Price to check
            
        Returns:
            True if price already exists, False otherwise
        """
        for level in levels:
            if self._prices_equal(level['price'], price):
                return True
        return False
    
    def _prices_equal(self, price1: float, price2: float) -> bool:
        """
        Check if two prices are equal within a tolerance
        
        Args:
            price1: First price
            price2: Second price
            
        Returns:
            True if prices are considered equal, False otherwise
        """
        # Calculate tolerance based on price magnitude
        tolerance = price1 * self.params['buffer'] / 100
        return abs(price1 - price2) <= tolerance
    
    def _consolidate_levels(self, levels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Consolidate nearby levels into zones
        
        Args:
            levels: List of level dictionaries
            
        Returns:
            Consolidated list of levels
        """
        if not levels:
            return []
        
        # Sort by price
        levels = sorted(levels, key=lambda x: x['price'])
        
        consolidated = [levels[0]]
        
        for level in levels[1:]:
            # Check if this level is close to the last consolidated level
            last = consolidated[-1]
            
            if self._prices_equal(level['price'], last['price']):
                # Merge by averaging price and taking max strength
                last['price'] = (last['price'] + level['price']) / 2
                last['strength'] = max(last['strength'], level['strength'])
                last['touches'] = last.get('touches', 0) + level.get('touches', 0)
                
                # Expand zone if needed
                last['zone'] = [
                    min(last['zone'][0], level['zone'][0]),
                    max(last['zone'][1], level['zone'][1])
                ]
                
                # Update source if needed
                if 'histogram' in level['source'] and 'swing' in last['source']:
                    # Prefer swing source for better description
                    pass
                elif 'fib' in level['source'] and 'swing' not in last['source']:
                    last['source'] = level['source']
            else:
                consolidated.append(level)
        
        return consolidated