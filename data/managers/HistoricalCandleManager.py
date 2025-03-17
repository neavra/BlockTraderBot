import asyncio
from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List, Optional, Tuple, Type
import time
from connectors.types import CandleSchema
from database.services.candleservice import CandleService
import colorama
from colorama import Fore, Style

class HistoricalCandleManager:
    """
    Manages fetching and maintaining historical candlestick data.
    """
    def __init__(
            self, 
            candle_service : CandleService, 
            rest_client : Type, 
            symbol : str, 
            exchange : str, 
            timeframes : List[str],
            rate_limit_per_minute: int = 300,
            max_retries: int = 3,
            retry_delay: int = 5,
            logger=None,
            log_level=logging.INFO
        ):
        
        self.candle_service = candle_service
        self.rest_client = rest_client
        self.symbol = symbol
        self.exchange = exchange
        self.timeframes = timeframes

        self.setup_logger(log_level)
        self.logger.info(f"{Fore.GREEN}{__name__} initialized{Style.RESET_ALL}")
        self.logger = logger or logging.getLogger(__name__)

        # Dict of rest clients for different timeframe intervals
        self.rest_clients: Dict[str, any] = {}

        # Rate limiting control
        self.api_call_times = []
        self.lock = asyncio.Lock()
        self.rate_limit_per_minute = rate_limit_per_minute

        # Retry mechanism:
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Cache for pending operations to avoid duplicate work
        self._ongoing_operations = set()

        # Initialize REST clients
        self.initialize_rest_clients()
    
    def setup_logger(self, log_level):
        """Set up the logger for the service"""
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f'{Fore.CYAN}[%(asctime)s]{Style.RESET_ALL} {Fore.MAGENTA}%(levelname)s{Style.RESET_ALL} - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(log_level)
    
    async def _check_rate_limit(self):
        """
        Implements rate limiting to prevent API throttling.
        Waits if necessary to stay under the rate limit.
        """
        async with self.lock:
            current_time = time.time()
            # Remove API calls older than 1 minute
            self.api_call_times = [t for t in self.api_call_times if current_time - t < 60]
            
            # Check if we need to wait
            if len(self.api_call_times) >= self.rate_limit_per_minute:
                wait_time = 60 - (current_time - self.api_call_times[0])
                if wait_time > 0:
                    self.logger.debug(f"Rate limit reached, waiting for {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)
                    
            # Register this API call
            self.api_call_times.append(time.time())

    def initialize_rest_clients(self):
        """
        Initializes REST client instances for each timeframe and stores them in the rest_clients dictionary.
        Each timeframe gets its own client instance to allow for parallel requests.
        
        Returns:
            Dict[str, any]: Dictionary of initialized REST clients by timeframe
        """
        for timeframe in self.timeframes:
            if timeframe not in self.rest_clients:
                # Create a new client instance for this timeframe
                # We'll use the class passed to the constructor
                client = self.rest_client(
                    symbol=self.symbol,
                    interval=timeframe,
                    candlestick_service = self.candle_service
                )
                
                # Store the client in our dictionary
                self.rest_clients[timeframe] = client
    
    async def populate_historical_data(
        self, 
        lookback_days: int = 30, 
        batch_size: int = 1500, 
    ) -> Dict[str, int]:
        """
        Fetches historical data for all configured timeframes.
        
        Args:
            lookback_days: Number of days to look back for historical data
            batch_size: Number of candles per API request
        
        Returns:
            Dictionary with timeframes as keys and number of candles loaded as values
        """
        results = {}
        
        # Get candles by timeframe
        for timeframe in self.timeframes:
            candle_count = await self._populate_timeframe(timeframe, lookback_days, batch_size)
            results[timeframe] = candle_count
        self.logger.info(f"Population complete!")
        return results
    
    async def _populate_timeframe(
        self, 
        timeframe: str, 
        lookback_days: int, 
        batch_size: int
    ) -> int:
        """
        Populates historical data for a specific timeframe.
        
        Returns:
            Number of candles loaded
        """
        # Generate a unique operation key
        op_key = f"populate_{self.symbol}_{self.exchange}_{timeframe}"
        if op_key in self._ongoing_operations:
            self.logger.info(f"Skipping duplicate population for {timeframe}, already in progress")
            return 0
            
        try:
            self._ongoing_operations.add(op_key)
            #print(latest_candle)
            # Get range of candles to populate for:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=lookback_days)
            
            loaded_candles = 0
            forward_complete = False
            backward_complete = False

            # ** Step 1: Check if data exists before fetching **
            latest_candle = await self.candle_service.get_latest_candle(self.symbol, self.exchange, timeframe)
            earliest_candle = await self.candle_service.get_earliest_candle(self.symbol, self.exchange, timeframe)

            data_exists = bool(latest_candle or earliest_candle)  # Flag to track if DB has any candles

            # ** Step 2: Fetch Forward Data (Most Recent Candles) **
            if latest_candle:
                latest_time = datetime.fromtimestamp(latest_candle["timestamp"].timestamp(), tz=timezone.utc)

                while not forward_complete:
                    if (end_time - latest_time).total_seconds() * 1000 > self._get_timeframe_ms(timeframe):
                        self.logger.info(f"Fetching forward data for {timeframe}")
                        loaded_forward = await self._fetch_forward(
                            timeframe, 
                            int(latest_time.timestamp() * 1000 + 1), 
                            batch_size
                        )
                        loaded_candles += loaded_forward

                        if loaded_forward < batch_size:  # No more candles left to fetch
                            forward_complete = True

                        # Refresh latest candle
                        latest_candle = await self.candle_service.get_latest_candle(self.symbol, self.exchange, timeframe)
                        latest_time = datetime.fromtimestamp(latest_candle["timestamp"].timestamp(), tz=timezone.utc) if latest_candle else None
                    else:
                        forward_complete = True

            # ** Step 3: Fetch Backward Data (Historical Candles) **
            if earliest_candle:
                earliest_time = datetime.fromtimestamp(earliest_candle["timestamp"].timestamp(), tz=timezone.utc)

                while not backward_complete:
                    if (earliest_time > start_time) and ((start_time - earliest_time).total_seconds() * 1000 > self._get_timeframe_ms(timeframe)):
                        self.logger.info(f"Fetching backward data for {timeframe}")
                        loaded_backward = await self._fetch_backward(
                            timeframe, 
                            int(start_time.timestamp() * 1000), 
                            int(earliest_time.timestamp() * 1000 - 1), 
                            batch_size
                        )
                        loaded_candles += loaded_backward

                        if loaded_backward < batch_size:  # No more candles left to fetch
                            backward_complete = True

                        # Refresh earliest candle
                        earliest_candle = await self.candle_service.get_earliest_candle(self.symbol, self.exchange, timeframe)
                        earliest_time = datetime.fromtimestamp(earliest_candle["timestamp"].timestamp(), tz=timezone.utc) if earliest_candle else None
                    else:
                        backward_complete = True

            # ** Step 4: Fetch Entire Range ONLY IF No Data Exists **
            if not data_exists:
                self.logger.info(f"No data found for {timeframe}, fetching entire range")

                start_ms = int(start_time.timestamp() * 1000)
                end_ms = int(end_time.timestamp() * 1000)

                while start_ms < end_ms:
                    loaded = await self._fetch_range(timeframe, start_ms, end_ms, batch_size)
                    loaded_candles += loaded

                    start_ms += loaded * self._get_timeframe_ms(timeframe)  # Move start forward

                    if (end_ms - start_ms) < self._get_timeframe_ms(timeframe) or loaded == 0:  # If we fetch less than batch size, we have all available data
                        break
            return loaded_candles
            
        finally:
            if op_key in self._ongoing_operations:
                self._ongoing_operations.remove(op_key)
    
    async def _fetch_range(
        self, 
        timeframe: str, 
        start_ms: int, 
        end_ms: int, 
        batch_size: int
    ) -> int:
        """
        Fetches candlestick data for a specific time range.
        
        Returns:
            Number of candles loaded
        """
        total_loaded = 0
        current_end = end_ms
        
        while current_end > start_ms:
            # Apply rate limiting
            await self._check_rate_limit()

            # Get the appropriate client for this timeframe
            client = self.rest_clients.get(timeframe)
            
            # If client doesn't exist yet, create it
            if not client:
                self.initialize_rest_clients()
                client = self.rest_clients.get(timeframe)
            
            try:
                for attempt in range(self.max_retries):
                    try:
                        # Fetch batch of candles
                        candles = await client.fetch_candlestick_data(
                            startTime=start_ms,
                            endTime=current_end,
                            limit=batch_size
                        )

                        # Check whether last candle has closed, if no we don't store it:
                        last_candle = candles[-1]
                        if last_candle.timestamp > datetime.now(timezone.utc):
                            self.logger.info(f"Candle with {last_candle.timestamp} for {timeframe} not closed, discarding...")
                            del candles[-1]
                        
                        if not candles:
                            self.logger.info(f"No more candles to fetch for {timeframe}")
                            return total_loaded
                            
                        
                        # Store in candle_service
                        await self.candle_service.add_candles(candles)

                        # Update for next iteration
                        oldest_candle_time : datetime = min(c.timestamp for c in candles)
                        
                        # Subtract the timeframe
                        oldest_candle_time -= timedelta(milliseconds=self._get_timeframe_ms(timeframe))

                        # Update current_end
                        current_end = oldest_candle_time - timedelta(milliseconds=1)

                        current_end = current_end.timestamp()
                        
                        total_loaded += len(candles)
                        self.logger.debug(f"Loaded {len(candles)} {timeframe} candles")
                        
                        # Break out of retry loop on success
                        break
                        
                    except Exception as e:
                        if attempt < self.max_retries - 1:
                            wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                            self.logger.warning(f"Error fetching {timeframe} candles, retrying in {wait_time}s: {str(e)}")
                            await asyncio.sleep(wait_time)
                        else:
                            self.logger.error(f"Failed to fetch {timeframe} candles after {self.max_retries} attempts: {str(e)}")
                            return total_loaded
                            
            except asyncio.CancelledError:
                self.logger.info(f"Candle fetching for {timeframe} was cancelled")
                raise
                
        return total_loaded
    
    async def _fetch_forward(self, timeframe: str, latest_timestamp: int, batch_size: int) -> int:
        """
        Fetches newer candles from the latest timestamp forward.
        
        Returns:
            Number of candles loaded
        """
        # Implementation similar to _fetch_range but for forward fetching
        # This would typically use a different API endpoint for newest candles
        await self._check_rate_limit()
        
        # Get the appropriate client for this timeframe
        client = self.rest_clients.get(timeframe)

        # If client doesn't exist yet, create it
        if not client:
            self.initialize_rest_clients()
            client = self.rest_clients.get(timeframe)
        
        try:
            candles : List[CandleSchema] = await client.fetch_candlestick_data(
                limit=batch_size,
                startTime=int(latest_timestamp)  # Start after the latest we have
            )

            # Check whether last candle has closed, if no we don't store it:
            last_candle = candles[-1]
            #print(last_candle.timestamp.tzinfo)
            if last_candle.timestamp > datetime.now(timezone.utc):
                del candles[-1]
            
            if not candles:
                self.logger.debug(f"No new {timeframe} candles to fetch")
                return 0
                
            # Store the candles
            await self.candle_service.add_candles(candles)
            
            return len(candles)
            
        except Exception as e:
            self.logger.error(f"Error fetching forward {timeframe} candles: {str(e)}")
            return 0
    
    async def _fetch_backward(
        self, 
        timeframe: str, 
        earliest_timestamp: Optional[int], 
        target_start_ms: int, 
        batch_size: int
    ) -> int:
        """
        Fetches older candles from the earliest timestamp backward.
        
        Returns:
            Number of candles loaded
        """
        # If we don't have an earliest timestamp, nothing to fetch backward from
        if not earliest_timestamp:
            return 0
            
        # Similar implementation to _fetch_range, but working backward
        return await self._fetch_range(timeframe, earliest_timestamp, target_start_ms, batch_size)
    
    async def detect_and_fill_gaps(
        self, 
        timeframe: str, 
        max_gaps_to_fill: int = 100,
        max_gap_size: int = 1000  # Maximum number of candles to fetch for a single gap
    ) -> List[Tuple[int, int, int]]:
        """
        Detects and fills gaps in historical data.
        
        Args:
            timeframe: Timeframe to check for gaps
            max_gaps_to_fill: Maximum number of gaps to fix in one run
            max_gap_size: Maximum number of candles to fetch for a single gap
            
        Returns:
            List of tuples (gap_start, gap_end, candles_filled) for each gap filled
        """
        # Generate a unique operation key
        op_key = f"fill_gaps_{self.symbol}_{self.exchange}_{timeframe}"
        if op_key in self._ongoing_operations:
            self.logger.info(f"Skipping duplicate gap filling for {timeframe}, already in progress")
            return []
            
        try:
            self._ongoing_operations.add(op_key)
            
            # Get gaps from candle_service ##TODO - Need to add this method:
            gaps = await self.candle_service.check_for_gaps(self.symbol, self.exchange, timeframe, max_gaps_to_fill)
            
            if not gaps:
                self.logger.debug(f"No gaps found for {timeframe}")
                return []
                
            self.logger.info(f"Found {len(gaps)} gaps in {timeframe} data")
            
            results = []
            
            # Get the appropriate client for this timeframe
            client = self.rest_clients.get(timeframe)

            # If client doesn't exist yet, create it
            if not client:
                self.initialize_rest_clients()
                client = self.rest_clients.get(timeframe)

            for gap_start, gap_end in gaps:
                # Calculate expected number of candles based on timeframe
                ms_per_candle = self._get_timeframe_ms(timeframe)
                expected_candles = (gap_end - gap_start) // ms_per_candle
                
                # Skip excessively large gaps
                if expected_candles > max_gap_size:
                    self.logger.warning(f"Gap from {gap_start} to {gap_end} is too large ({expected_candles} candles), skipping")
                    continue
                
                # Fill the gap
                try:
                    await self._check_rate_limit()
                    candles = await client.fetch_candlestick_data(
                        startTime=gap_start,
                        endTime=gap_end,
                        limit=max_gap_size
                    )
                    
                    if candles:
                        await self.candle_service.add_candles(candles)
                        results.append((gap_start, gap_end, len(candles)))
                        self.logger.info(f"Filled gap in {timeframe} from {gap_start} to {gap_end} with {len(candles)} candles")
                    else:
                        self.logger.warning(f"No candles returned for gap in {timeframe} from {gap_start} to {gap_end}")
                        
                except Exception as e:
                    self.logger.error(f"Error filling gap in {timeframe} from {gap_start} to {gap_end}: {str(e)}")
                    
            return results
            
        finally:
            if op_key in self._ongoing_operations:
                self._ongoing_operations.remove(op_key)
    
    async def run_maintenance(
        self, 
        interval: int = 3600,
        lookback_days: int = 30,
        gap_check_interval: int = 7200,  # Check for gaps once per 2 hours
        auto_restart: bool = True
    ) -> None:
        """
        Runs periodic maintenance tasks to keep historical data up-to-date.
        
        Args:
            interval: Seconds between update checks
            lookback_days: Number of days to look back for historical data
            gap_check_interval: Seconds between gap checks
            auto_restart: Whether to auto-restart the task if it fails
        """
        last_gap_check = 0
        
        # Sleep the first maintenance interval
        await asyncio.sleep(interval)
        while True:
            try:
                # Check if we need to run a gap detection pass
                current_time = time.time()
                run_gap_check = (current_time - last_gap_check) >= gap_check_interval
                
                for timeframe in self.timeframes:
                    # Update recent data
                    self.logger.info(f"Running maintenance update for {timeframe}")
                    await self._populate_timeframe(timeframe, lookback_days=lookback_days, batch_size=1500)
                    """latest_candle = self.candle_service.get_latest_candle(
                        timeframe=timeframe,
                        symbol=self.symbol,
                        exchange=self.exchange
                        )
                    
                    if latest_candle:
                        await self._fetch_forward(timeframe=timeframe, latest_timestamp=latest_candle["timestamp"].timestamp(), batch_size=1500)
                    else:
                        # If no data exists, populate with default lookback
                        self.logger.warning(f"No data found for {timeframe} during maintenance, populating with default lookback")
                        await self._populate_timeframe(timeframe, lookback_days=30, batch_size=1500)"""
                    
                    # Check for gaps if needed
                    if run_gap_check:
                        self.logger.info(f"Running gap check for {timeframe}")
                        await self.detect_and_fill_gaps(timeframe)
                
                # Update last gap check time
                if run_gap_check:
                    last_gap_check = current_time
                
                # Wait for next maintenance interval
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                self.logger.info("Maintenance task cancelled")
                raise
                
            except Exception as e:
                self.logger.exception(f"Error in maintenance task: {str(e)}")
                #self.logger.error(f"Error in maintenance task: {str(e)}")
                if auto_restart:
                    self.logger.info(f"Restarting maintenance task in {interval} seconds")
                    await asyncio.sleep(interval)
                else:
                    raise
    
    def start_maintenance(self, lookback_days: int = 30, interval: int = 3600) -> asyncio.Task:
        """
        Starts the maintenance task in the background.

        Args:
            lookback_days: Number of days to look back for historical data
            interval: Seconds between update checks
        
        Returns:
            asyncio.Task: The maintenance task
        """
        if self._maintenance_task and not self._maintenance_task.done():
            self.logger.warning("Maintenance task already running")
            return self._maintenance_task
            
        self._maintenance_task = asyncio.create_task(self.run_maintenance(interval, lookback_days))
        return self._maintenance_task
    
    def stop_maintenance(self) -> None:
        """
        Stops the maintenance task if it's running.
        """
        if self._maintenance_task and not self._maintenance_task.done():
            self._maintenance_task.cancel()
            self.logger.info("Maintenance task cancelled")
    
    def _get_timeframe_ms(self, timeframe: str) -> int:
        """
        Converts a timeframe string to milliseconds.
        
        Examples:
            "1m" -> 60000
            "1h" -> 3600000
        """
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        
        if unit == 'm':
            return value * 60 * 1000
        elif unit == 'h':
            return value * 60 * 60 * 1000
        elif unit == 'd':
            return value * 24 * 60 * 60 * 1000
        elif unit == 'w':
            return value * 7 * 24 * 60 * 60 * 1000
        elif unit == 'M':
            return value * 4 * 7 * 24 * 60 * 60 * 1000
        else:
            raise ValueError(f"Unsupported timeframe unit: {unit}")
        
    async def fill_to_current_date(self, timeframe: str, max_requests: int = 10) -> int:
        """
        Fills candlestick data from the latest available candle to the current date.
        
        Args:
            timeframe: Timeframe to fill
            max_requests: Maximum number of API requests to make (safety limit)
            
        Returns:
            Number of candles loaded
        """
        # Get the latest candle
        latest_candle = await self.candle_service.get_latest_candle(symbol=self.symbol, exchange=self.exchange, timeframe=timeframe)
        
        if not latest_candle:
            self.logger.warning(f"No existing data for {timeframe}, cannot fill forward")
            return 0
        
        # Calculate current timestamp
        current_ms = int(datetime.now().timestamp() * 1000)
        
        # Don't fill if we already have data up to the current time
        # Allow for a small gap (one candle period) to account for incomplete candles
        candle_period_ms = self._get_timeframe_ms(timeframe)
        if latest_candle.close_time >= current_ms - candle_period_ms:
            self.logger.info(f"Data for {timeframe} is already up to date")
            return 0
        
        # Fill forward in batches
        total_candles = 0
        current_start = latest_candle.close_time + 1
        request_count = 0
        
        while current_start < current_ms and request_count < max_requests:
            # Apply rate limiting
            await self._check_rate_limit()
            
            # Fetch the next batch
            loaded_candles = await self._fetch_forward(timeframe, current_start, batch_size=1500)
            
            if loaded_candles == 0:
                # No more candles available
                break
            
            total_candles += loaded_candles
            request_count += 1
            
            # Update the starting point for the next batch
            # We need to get the latest candle again since _fetch_forward has updated the DB
            latest_candle = await self.repository.get_latest_candle(self.symbol, self.exchange, timeframe)
            if not latest_candle:
                break
            
            current_start = latest_candle.close_time + 1
        
        if request_count >= max_requests:
            self.logger.warning(f"Reached maximum number of requests ({max_requests}) for {timeframe}")
        
        self.logger.info(f"Filled {total_candles} candles for {timeframe} up to current date")
        return total_candles

    async def validate_data_integrity(self, timeframe: str, start_time: int, end_time: int) -> Dict:
        """
        Validates the integrity of stored candle data.
        
        Returns:
            Dictionary with validation results
        """
        # Get all candles in the range
        candles = await self.candle_service.get_candles(
            self.symbol, 
            self.exchange, 
            timeframe, 
            start_time=start_time, 
            end_time=end_time
        )
        
        if not candles:
            return {
                "valid": False,
                "message": "No candles found in the specified range",
                "missing_candles": None,
                "duplicate_candles": None
            }
        
        # Sort candles by open_time
        candles.sort(key=lambda c: c.open_time)
        
        # Check for duplicate candles
        open_times = [c.open_time for c in candles]
        duplicates = set([t for t in open_times if open_times.count(t) > 1])
        
        # Check for missing candles
        ms_per_candle = self._get_timeframe_ms(timeframe)
        expected_candle_times = set(range(
            candles[0].open_time, 
            candles[-1].open_time + ms_per_candle, 
            ms_per_candle
        ))
        actual_candle_times = set(open_times)
        missing_times = expected_candle_times - actual_candle_times
        
        return {
            "valid": len(duplicates) == 0 and len(missing_times) == 0,
            "message": "Data validation complete",
            "total_candles": len(candles),
            "duplicate_candles": sorted(list(duplicates)) if duplicates else None,
            "missing_candles": sorted(list(missing_times)) if missing_times else None,
            "earliest_candle": candles[0].open_time if candles else None,
            "latest_candle": candles[-1].open_time if candles else None
        }
    
    async def optimize_storage(self, timeframe: str, older_than_days: int = 30) -> int:
        """
        Optimizes storage by cleaning up or compressing older data.
        
        Args:
            timeframe: Timeframe to optimize
            older_than_days: Process data older than this many days
            
        Returns:
            Number of candles processed
        """
        # Calculate cutoff time
        cutoff_time = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).timestamp() * 1000
        
        # Get older candles
        old_candles = await self.candle_service.get_candles(
            self.symbol,
            self.exchange,
            timeframe,
            end_time=cutoff_time
        )
        
        if not old_candles:
            self.logger.info(f"No old {timeframe} candles to optimize")
            return 0
            
        # Here you would implement your optimization strategy
        # For example, you might:
        # 1. Compress data
        # 2. Move to cold storage
        # 3. Reduce precision
        # 4. etc.
        
        # Placeholder for optimization logic
        self.logger.info(f"Optimized {len(old_candles)} old {timeframe} candles")
        return len(old_candles)
    
    def _convert_to_candle_data(self, candle_dict: Dict, timeframe: str) -> CandleSchema:
        """
        Converts exchange-specific candle format to our internal CandleData format.
        """
        # This is a placeholder - actual implementation would depend on the exchange format
        return CandleSchema(
            symbol=self.symbol,
            exchange=self.exchange,
            timeframe=timeframe,
            open_time=candle_dict['open_time'],
            close_time=candle_dict['close_time'],
            open=float(candle_dict['open']),
            high=float(candle_dict['high']),
            low=float(candle_dict['low']),
            close=float(candle_dict['close']),
            volume=float(candle_dict['volume']),
            trade_count=candle_dict.get('trade_count'),
            completed=candle_dict.get('completed', True)
        )
    
    async def backfill_to_date(self, timeframe: str, target_date: datetime) -> int:
        """
        Backfills historical data to a specific date.
        
        Args:
            timeframe: Timeframe to backfill
            target_date: Date to backfill to
            
        Returns:
            Number of candles loaded
        """
        # Get the earliest candle
        earliest_candle = await self.candle_service.get_earliest_candle(self.symbol, self.exchange, timeframe)
        
        if not earliest_candle:
            self.logger.warning(f"No existing data for {timeframe}, cannot backfill")
            return 0
            
        # Calculate target timestamp
        target_ms = int(target_date.timestamp() * 1000)
        
        # Don't backfill if we already have data going back far enough
        if earliest_candle.open_time <= target_ms:
            self.logger.info(f"Already have {timeframe} data back to {target_date}")
            return 0
            
        # Backfill in batches
        return await self._fetch_backward(timeframe, earliest_candle.open_time, target_ms, batch_size=1500)
    
    async def synchronize_timeframes(self, base_timeframe: str, target_timeframes: List[str]) -> Dict[str, int]:
        """
        Ensures all target timeframes have data for the same time range as the base timeframe.
        
        Args:
            base_timeframe: Reference timeframe
            target_timeframes: Timeframes to synchronize
            
        Returns:
            Dictionary with timeframes as keys and number of candles loaded as values
        """
        # Get the earliest and latest candles for base timeframe
        earliest_base = await self.candle_service.get_earliest_candle(self.symbol, self.exchange, base_timeframe)
        latest_base = await self.candle_service.get_latest_candle(self.symbol, self.exchange, base_timeframe)
        
        if not earliest_base or not latest_base:
            self.logger.warning(f"No data for base timeframe {base_timeframe}, cannot synchronize")
            return {}
            
        results = {}
        
        for timeframe in target_timeframes:
            if timeframe == base_timeframe:
                continue
                
            # Get earliest and latest for this timeframe
            earliest_target = await self.candle_service.get_earliest_candle(self.symbol, self.exchange, timeframe)
            latest_target = await self.candle_service.get_latest_candle(self.symbol, self.exchange, timeframe)
            
            candles_loaded = 0
            
            # Backfill if needed
            if not earliest_target or earliest_target.open_time > earliest_base.open_time:
                start_time = earliest_base.open_time
                end_time = earliest_target.open_time - 1 if earliest_target else latest_base.open_time
                
                self.logger.info(f"Backfilling {timeframe} from {start_time} to {end_time}")
                candles_loaded += await self._fetch_range(timeframe, start_time, end_time, batch_size=1500)
            
            # Forward fill if needed
            if not latest_target or latest_target.close_time < latest_base.close_time:
                start_time = (latest_target.close_time + 1) if latest_target else earliest_base.open_time
                end_time = latest_base.close_time
                
                self.logger.info(f"Forward filling {timeframe} from {start_time} to {end_time}")
                candles_loaded += await self._fetch_range(timeframe, start_time, end_time, batch_size=1500)
                
            results[timeframe] = candles_loaded
            
        return results
