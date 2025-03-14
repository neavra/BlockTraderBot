import time


class CustomTFCandleManager:
    """
    Calculates custom timeframes from base timeframes, stores them in the database,
    and maintains them over time.
    """
    def __init__(self, candle_service, symbol, exchange, base_timeframes, custom_timeframes, timeframe_derivation_map=None):
        self.candle_service = candle_service
        self.symbol = symbol
        self.exchange = exchange
        self.base_timeframes = base_timeframes
        self.custom_timeframes = custom_timeframes
        
        # Maps each custom timeframe to the base timeframe it should be derived from
        # For optimal performance, each custom timeframe should be derived from the largest
        # base timeframe that divides evenly into it
        self.timeframe_derivation_map = timeframe_derivation_map or self._create_default_derivation_map()
        
        # Cache for tracking open custom candles
        self.open_custom_candles = {}
        
        # Conversion helpers
        self.timeframe_to_seconds = {
            "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
            "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
            "8h": 28800, "12h": 43200, "1d": 86400
        }
    
    def _create_default_derivation_map(self):
        """
        Creates an optimal mapping of custom timeframes to source base timeframes.
        """
        derivation_map = {}
        
        # Convert timeframes to seconds for calculations
        base_tf_seconds = {tf: self._timeframe_to_seconds(tf) for tf in self.base_timeframes}
        custom_tf_seconds = {tf: self._timeframe_to_seconds(tf) for tf in self.custom_timeframes}
        
        # For each custom timeframe, find the largest base timeframe that divides evenly into it
        for custom_tf, custom_seconds in custom_tf_seconds.items():
            best_base_tf = None
            best_base_seconds = 0
            
            for base_tf, base_seconds in base_tf_seconds.items():
                if custom_seconds % base_seconds == 0 and base_seconds > best_base_seconds:
                    best_base_tf = base_tf
                    best_base_seconds = base_seconds
            
            if best_base_tf:
                derivation_map[custom_tf] = best_base_tf
            else:
                # Fallback to the smallest base timeframe if no exact division
                derivation_map[custom_tf] = min(base_tf_seconds, key=base_tf_seconds.get)
        
        return derivation_map
    
    def _timeframe_to_seconds(self, timeframe):
        """Convert timeframe string to seconds."""
        if timeframe in self.timeframe_to_seconds:
            return self.timeframe_to_seconds[timeframe]
        
        # Parse timeframe in format like "1m", "4h", "1d"
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        
        if unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400
        elif unit == 'w':
            return value * 86400 * 7
        else:
            raise ValueError(f"Unknown timeframe unit: {unit}")
    
    def _calculate_candle_timeframe_timestamp(self, timestamp, timeframe):
        """
        Calculate the start timestamp for a candle of a given timeframe.
        This ensures that candles are properly aligned regardless of when calculation occurs.
        """
        seconds = self._timeframe_to_seconds(timeframe)
        return (timestamp // seconds) * seconds
    
    async def initialize_custom_timeframes(self, start_time=None, end_time=None):
        """
        Perform the initial calculation of all custom timeframes based on historical data.
        This should be called once during system startup to populate the database.
        """
        for custom_tf in self.custom_timeframes:
            # Check if custom timeframe data already exists
            existing_candles = await self.candle_service.get_candles(
                self.symbol, self.exchange, custom_tf, limit=1
            )
            
            if existing_candles:
                print(f"Custom timeframe {custom_tf} already exists in database, checking for gaps...")
                # Check for gaps in existing custom timeframe data
                await self.fill_custom_timeframe_gaps(custom_tf, start_time, end_time)
            else:
                print(f"Calculating custom timeframe {custom_tf} from scratch...")
                # Calculate the custom timeframe from scratch
                await self.calculate_custom_timeframe(custom_tf, start_time, end_time)
    
    async def calculate_custom_timeframe(self, custom_timeframe, start_time=None, end_time=None):
        """
        Calculate a specific custom timeframe from its base timeframe and store in the database.
        """
        base_timeframe = self.timeframe_derivation_map[custom_timeframe]
        custom_seconds = self._timeframe_to_seconds(custom_timeframe)
        base_seconds = self._timeframe_to_seconds(base_timeframe)
        candles_per_custom = custom_seconds // base_seconds
        
        # Get all base timeframe candles within the specified time range
        base_candles = await self.candle_service.get_candles(
            self.symbol, self.exchange, base_timeframe,
            start_time=start_time, end_time=end_time
        )
        
        if not base_candles:
            print(f"No base candles found for {base_timeframe} to calculate {custom_timeframe}")
            return
        
        # Group candles by their custom timeframe timestamp
        grouped_candles = {}
        for candle in base_candles:
            # Calculate which custom candle this base candle belongs to
            custom_timestamp = self._calculate_candle_timeframe_timestamp(candle['timestamp'], custom_timeframe)
            
            if custom_timestamp not in grouped_candles:
                grouped_candles[custom_timestamp] = []
            
            grouped_candles[custom_timestamp].append(candle)
        
        # Create and store custom candles
        custom_candles = []
        for timestamp, candles in grouped_candles.items():
            # Only create a custom candle if we have all required base candles
            if len(candles) == candles_per_custom:
                custom_candle = self._aggregate_candles(candles, custom_timeframe)
                custom_candles.append(custom_candle)
        
        # Save custom candles to the database in batches
        if custom_candles:
            await self.candle_service.add_candles(custom_candles)
            print(f"Added {len(custom_candles)} {custom_timeframe} candles to the database")
    
    async def fill_custom_timeframe_gaps(self, custom_timeframe, start_time=None, end_time=None):
        """
        Detect and fill gaps in a custom timeframe's data.
        """
        # Get gaps in the custom timeframe data
        gaps = await self.candle_service.check_for_gaps(
            self.symbol, self.exchange, custom_timeframe,
            start_time=start_time, end_time=end_time
        )
        
        if not gaps:
            print(f"No gaps found in {custom_timeframe} data")
            return
        
        # Fill each gap
        for gap in gaps:
            gap_start, gap_end = gap
            print(f"Filling gap in {custom_timeframe} from {gap_start} to {gap_end}")
            await self.calculate_custom_timeframe(
                custom_timeframe, start_time=gap_start, end_time=gap_end
            )
    
    async def update_from_new_candle(self, candle_data):
        """
        Update custom timeframes based on a newly received candle.
        This should be called whenever a new base timeframe candle is received.
        """
        timeframe = candle_data.get('timeframe')
        
        # Skip if this is not a base timeframe or not needed for any custom timeframes
        if timeframe not in self.base_timeframes:
            return
        
        # Find which custom timeframes need to be updated based on this base timeframe
        for custom_tf, base_tf in self.timeframe_derivation_map.items():
            if base_tf == timeframe:
                await self._update_custom_timeframe(custom_tf, candle_data)
    
    async def _update_custom_timeframe(self, custom_timeframe, candle_data):
        """
        Update a specific custom timeframe based on a new base candle.
        """
        base_timeframe = candle_data['timeframe']
        base_timestamp = candle_data['timestamp']
        
        # Calculate which custom candle this base candle belongs to
        custom_timestamp = self._calculate_candle_timeframe_timestamp(base_timestamp, custom_timeframe)
        custom_id = f"{self.symbol}_{self.exchange}_{custom_timeframe}_{custom_timestamp}"
        
        # Check if we need to create or update a custom candle
        if custom_id not in self.open_custom_candles:
            # Check if this custom candle already exists in the database
            existing_candle = await self.candle_service.get_candle_by_timestamp(
                self.symbol, self.exchange, custom_timeframe, custom_timestamp
            )
            
            if existing_candle:
                # Use existing candle as a starting point
                self.open_custom_candles[custom_id] = existing_candle
            else:
                # Create a new custom candle
                self.open_custom_candles[custom_id] = {
                    'symbol': self.symbol,
                    'exchange': self.exchange,
                    'timeframe': custom_timeframe,
                    'timestamp': custom_timestamp,
                    'open': candle_data['open'],
                    'high': candle_data['high'],
                    'low': candle_data['low'],
                    'close': candle_data['close'],
                    'volume': candle_data['volume'],
                    'component_candles': [base_timestamp],
                    'complete': False
                }
        else:
            # Update existing custom candle
            custom_candle = self.open_custom_candles[custom_id]
            custom_candle['high'] = max(custom_candle['high'], candle_data['high'])
            custom_candle['low'] = min(custom_candle['low'], candle_data['low'])
            custom_candle['close'] = candle_data['close']
            custom_candle['volume'] += candle_data['volume']
            custom_candle['component_candles'].append(base_timestamp)
        
        # Check if the custom candle is now complete
        base_candles_needed = self._timeframe_to_seconds(custom_timeframe) // self._timeframe_to_seconds(base_timeframe)
        if len(self.open_custom_candles[custom_id]['component_candles']) >= base_candles_needed:
            self.open_custom_candles[custom_id]['complete'] = True
            
            # Store the completed custom candle
            await self.candle_service.update_candle(self.open_custom_candles[custom_id])
            
            # Remove from open candles cache
            # If this is the last base candle for the timeframe
            end_timestamp = custom_timestamp + self._timeframe_to_seconds(custom_timeframe)
            current_time = int(time.time())
            if current_time >= end_timestamp:
                del self.open_custom_candles[custom_id]
    
    def _aggregate_candles(self, candles, custom_timeframe):
        """
        Aggregate multiple base timeframe candles into a single custom timeframe candle.
        """
        if not candles:
            return None
        
        # Sort candles by timestamp to ensure correct order
        sorted_candles = sorted(candles, key=lambda x: x['timestamp'])
        
        # Get the first and last candle
        first_candle = sorted_candles[0]
        last_candle = sorted_candles[-1]
        
        # Calculate custom candle timestamp (aligned to the custom timeframe)
        custom_timestamp = self._calculate_candle_timeframe_timestamp(first_candle['timestamp'], custom_timeframe)
        
        # Calculate OHLCV values
        custom_candle = {
            'symbol': self.symbol,
            'exchange': self.exchange,
            'timeframe': custom_timeframe,
            'timestamp': custom_timestamp,
            'open': first_candle['open'],
            'high': max(candle['high'] for candle in sorted_candles),
            'low': min(candle['low'] for candle in sorted_candles),
            'close': last_candle['close'],
            'volume': sum(candle['volume'] for candle in sorted_candles),
            'component_candles': [candle['timestamp'] for candle in sorted_candles],
            'complete': True
        }
        
        return custom_candle
    
    async def get_custom_timeframe_candles(self, timeframe, start_time=None, end_time=None, limit=None):
        """
        Get candles for a custom timeframe. This is a wrapper around the candle_service's get_candles
        method to ensure we're retrieving the correct data.
        """
        if timeframe not in self.custom_timeframes:
            raise ValueError(f"Unknown custom timeframe: {timeframe}")
        
        return await self.candle_service.get_candles(
            self.symbol, self.exchange, timeframe,
            start_time=start_time, end_time=end_time, limit=limit
        )
    
    async def clean_stale_candles(self, max_age_hours=24):
        """
        Clean up any stale custom candles in the in-memory cache.
        Should be called periodically to prevent memory leaks.
        """
        current_time = int(time.time())
        stale_threshold = current_time - (max_age_hours * 3600)
        
        stale_keys = [
            key for key, candle in self.open_custom_candles.items()
            if candle['timestamp'] < stale_threshold
        ]
        
        for key in stale_keys:
            del self.open_custom_candles[key]
        
        if stale_keys:
            print(f"Cleaned {len(stale_keys)} stale custom candles from cache")