import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from models.candlestick import Candlestick
import json
import pandas as pd
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

class CandleDBManager:
    """
    Manager class for handling the integration of historical and live candlestick data.
    Provides seamless database population and gap-filling capabilities.
    """
    
    def __init__(self, db_adapter, candle_service, rest_client, symbol, exchange, timeframes, log_level=logging.INFO):
        """
        Initialize the CandleDBManager.
        
        Args:
            db_adapter: Database adapter for DB connections
            candle_service: Service for candlestick CRUD operations
            rest_client: REST API client for fetching historical data
            symbol: Trading symbol (e.g., 'BTCUSDT')
            exchange: Exchange name (e.g., 'binance')
            timeframes: List of timeframes to manage (e.g., ['1m', '5m', '1h'])
            log_level: Logging level
        """
        self.db_adapter = db_adapter
        self.candle_service = candle_service
        self.rest_client = rest_client
        self.symbol = symbol
        self.exchange = exchange
        self.timeframes = timeframes
        self.setup_logger(log_level)
        
        # In-memory cache for open candles
        self.open_candles = {}
        for tf in timeframes:
            self.open_candles[tf] = {}
            
        self.logger.info(f"{Fore.GREEN}CandleDBManager initialized for {symbol} on {exchange}{Style.RESET_ALL}")
        self.logger.info(f"Managing timeframes: {', '.join(timeframes)}")
    
    def setup_logger(self, log_level):
        """Set up the logger for the manager"""
        self.logger = logging.getLogger(f"CandleDBManager_{self.symbol}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f'{Fore.CYAN}[%(asctime)s]{Style.RESET_ALL} {Fore.MAGENTA}%(name)s{Style.RESET_ALL} - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(log_level)
    
    def _parse_timeframe(self, timeframe: str) -> tuple:
        """
        Parse a timeframe string into value and unit.
        
        Args:
            timeframe: Timeframe string (e.g., '1m', '4h', '1d')
            
        Returns:
            tuple: (value, unit) e.g., (1, 'm') for '1m'
        """
        value = int(timeframe[:-1])
        unit = timeframe[-1]
        return value, unit
    
    def _get_timeframe_seconds(self, timeframe: str) -> int:
        """
        Convert timeframe to seconds.
        
        Args:
            timeframe: Timeframe string (e.g., '1m', '4h', '1d')
            
        Returns:
            int: Number of seconds in the timeframe
        """
        value, unit = self._parse_timeframe(timeframe)
        
        if unit == 's':
            return value
        elif unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 60 * 60
        elif unit == 'd':
            return value * 24 * 60 * 60
        elif unit == 'w':
            return value * 7 * 24 * 60 * 60
        elif unit == 'M':  # Month (approximate)
            return value * 30 * 24 * 60 * 60
        else:
            raise ValueError(f"Unknown timeframe unit: {unit}")
    
    def _calculate_candle_limit(self, timeframe: str, lookback_days: int) -> int:
        """
        Calculate how many candles we need for a given lookback period.
        
        Args:
            timeframe: Timeframe string (e.g., '1m', '4h', '1d')
            lookback_days: Number of days to look back
            
        Returns:
            int: Number of candles needed
        """
        seconds_per_candle = self._get_timeframe_seconds(timeframe)
        seconds_in_lookback = lookback_days * 24 * 60 * 60
        return int(seconds_in_lookback / seconds_per_candle)
    
    def _timestamp_to_datetime(self, timestamp) -> datetime:
        """
        Convert a timestamp to a datetime object.
        
        Args:
            timestamp: Timestamp (can be in ms or seconds)
            
        Returns:
            datetime: Converted datetime object
        """
        if isinstance(timestamp, (int, float)):
            # Convert to seconds if in milliseconds
            if timestamp > 1e12:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp)
        return timestamp
    
    async def _get_latest_candle(self, timeframe: str) -> Optional[Dict]:
        """
        Get the most recent candle for a timeframe.
        
        Args:
            timeframe: Timeframe to check
            
        Returns:
            Dict or None: Latest candle data or None if no candles exist
        """
        candles = self.candle_service.get_candles(
            self.symbol, self.exchange, timeframe, limit=1
        )
        
        if candles and len(candles) > 0:
            # Convert to dict if it's an ORM object
            if hasattr(candles[0], '__dict__'):
                return {k: v for k, v in candles[0].__dict__.items() if not k.startswith('_')}
            return candles[0]
        return None
    
    async def _get_earliest_candle(self, timeframe: str) -> Optional[Dict]:
        """
        Get the earliest candle for a timeframe.
        
        Args:
            timeframe: Timeframe to check
            
        Returns:
            Dict or None: Earliest candle data or None if no candles exist
        """
        with self.db_adapter.get_db() as session:
            try:
                # Query the earliest candle by ordering by timestamp ascending
                earliest = session.query(self.candle_service.Candlestick).filter_by(
                    symbol=self.symbol, exchange=self.exchange, timeframe=timeframe
                ).order_by(self.candle_service.Candlestick.timestamp.asc()).first()
                
                if earliest:
                    # Convert to dict if it's an ORM object
                    if hasattr(earliest, '__dict__'):
                        return {k: v for k, v in earliest.__dict__.items() if not k.startswith('_')}
                    return earliest
                return None
            except Exception as e:
                self.logger.error(f"{Fore.RED}Error getting earliest candle: {e}{Style.RESET_ALL}")
                return None
    
    async def populate_historical_data(self, lookback_days: int = 30, batch_size: int = 1000) -> None:
        """
        Populate database with historical data for all timeframes.
        
        Args:
            lookback_days: Number of days to look back (default: 30)
            batch_size: Number of candles to fetch in each batch (default: 1000)
        """
        self.logger.info(f"{Fore.BLUE}Starting historical data population...{Style.RESET_ALL}")
        
        for timeframe in self.timeframes:
            self.logger.info(f"Processing timeframe: {timeframe}")
            
            # Check if we already have data for this timeframe
            latest_candle = await self._get_latest_candle(timeframe)
            
            if latest_candle:
                # We have existing data, fetch only the missing data
                latest_timestamp = latest_candle['timestamp']
                self.logger.info(
                    f"Found existing data for {timeframe}, latest timestamp: "
                    f"{self._timestamp_to_datetime(latest_timestamp)}"
                )
                
                # Fetch data from latest candle to now
                current_time = int(time.time() * 1000)  # Current time in ms
                
                try:
                    historical_data = await self.rest_client.fetch_candlestick_data_range(
                        self.symbol, timeframe, 
                        start_time=latest_timestamp + 1,  # +1 to avoid duplication
                        end_time=current_time
                    )
                    
                    if historical_data and len(historical_data) > 0:
                        # Process the historical data
                        self.logger.info(
                            f"{Fore.GREEN}Fetched {len(historical_data)} new candles for {timeframe} "
                            f"from {self._timestamp_to_datetime(historical_data[0]['timestamp'])} "
                            f"to {self._timestamp_to_datetime(historical_data[-1]['timestamp'])}{Style.RESET_ALL}"
                        )
                        
                        # Add exchange information if not present
                        for candle in historical_data:
                            if 'exchange' not in candle:
                                candle['exchange'] = self.exchange
                        
                        # Store in database
                        self.candle_service.add_candles(historical_data)
                    else:
                        self.logger.info(f"No new data to fetch for {timeframe}")
                        
                except Exception as e:
                    self.logger.error(f"{Fore.RED}Error fetching data for {timeframe}: {e}{Style.RESET_ALL}")
            else:
                # No existing data, fetch the full lookback period
                self.logger.info(f"No existing data found for {timeframe}, fetching {lookback_days} days of history")
                
                # Calculate how many candles we need based on lookback period
                limit = self._calculate_candle_limit(timeframe, lookback_days)
                
                # Fetch in batches to avoid overloading
                total_fetched = 0
                current_limit = min(batch_size, limit)
                end_time = int(time.time() * 1000)  # Current time in ms
                
                while total_fetched < limit:
                    try:
                        historical_data = await self.rest_client.fetch_candlestick_data(
                            self.symbol, timeframe, limit=current_limit, end_time=end_time
                        )
                        
                        if not historical_data or len(historical_data) == 0:
                            self.logger.info(f"No more historical data available for {timeframe}")
                            break
                            
                        # Add exchange information if not present
                        for candle in historical_data:
                            if 'exchange' not in candle:
                                candle['exchange'] = self.exchange
                        
                        # Store in database
                        self.candle_service.add_candles(historical_data)
                        
                        total_fetched += len(historical_data)
                        self.logger.info(
                            f"{Fore.GREEN}Fetched {len(historical_data)} historical candles for {timeframe} "
                            f"({total_fetched}/{limit}){Style.RESET_ALL}"
                        )
                        
                        # Update end_time for next batch (use oldest timestamp - 1)
                        if len(historical_data) > 0:
                            # Find the oldest timestamp
                            oldest_timestamp = min(candle['timestamp'] for candle in historical_data)
                            end_time = oldest_timestamp - 1
                        else:
                            break
                            
                    except Exception as e:
                        self.logger.error(f"{Fore.RED}Error fetching historical data for {timeframe}: {e}{Style.RESET_ALL}")
                        break
                        
                    # Slight delay to avoid rate limits
                    await asyncio.sleep(0.5)
                    
        self.logger.info(f"{Fore.GREEN}Historical data population completed!{Style.RESET_ALL}")
    
    async def detect_and_fill_gaps(self, timeframe: str, max_gaps_to_fill: int = 100) -> int:
        """
        Detect gaps in data and fill them from the REST API.
        
        Args:
            timeframe: Timeframe to check for gaps
            max_gaps_to_fill: Maximum number of gaps to fill in one run
            
        Returns:
            int: Number of gaps filled
        """
        self.logger.info(f"Detecting gaps for {self.symbol}/{timeframe}...")
        
        # Get all candles for this timeframe/symbol, ordered by timestamp
        with self.db_adapter.get_db() as session:
            try:
                # This might need adjustment based on your actual database model
                candles = session.query(Candlestick).filter_by(
                    symbol=self.symbol, exchange=self.exchange, timeframe=timeframe
                ).order_by(Candlestick.timestamp.asc()).all()
                
                # Convert to dicts if they're ORM objects
                candles_data = []
                for c in candles:
                    if hasattr(c, '__dict__'):
                        candles_data.append({k: v for k, v in c.__dict__.items() if not k.startswith('_')})
                    else:
                        candles_data.append(c)
                
                self.logger.info(f"Found {len(candles_data)} candles to analyze for gaps")
                
                # Expected time difference between candles
                expected_diff = self._get_timeframe_seconds(timeframe) * 1000  # in ms
                
                # Check for gaps
                gaps = []
                for i in range(1, len(candles_data)):
                    current_timestamp = candles_data[i]['timestamp']
                    prev_timestamp = candles_data[i-1]['timestamp']
                    actual_diff = current_timestamp - prev_timestamp
                    
                    # If the difference is significantly larger than expected, we have a gap
                    if actual_diff > expected_diff * 1.5:
                        # Calculate how many candles we're missing
                        missing_count = int(actual_diff / expected_diff) - 1
                        start_time = prev_timestamp + expected_diff
                        end_time = current_timestamp - expected_diff
                        
                        gaps.append({
                            'start': start_time,
                            'end': end_time,
                            'count': missing_count
                        })
                        
                        self.logger.info(
                            f"Gap detected: {self._timestamp_to_datetime(start_time)} to "
                            f"{self._timestamp_to_datetime(end_time)} ({missing_count} candles missing)"
                        )
                
                self.logger.info(f"Found {len(gaps)} gaps for {self.symbol}/{timeframe}")
                
                # Limit the number of gaps to fill at once to avoid overwhelming the API
                gaps_to_fill = gaps[:max_gaps_to_fill]
                gaps_filled = 0
                
                # Fill gaps
                for gap in gaps_to_fill:
                    try:
                        missing_data = await self.rest_client.fetch_candlestick_data_range(
                            self.symbol, timeframe, 
                            start_time=gap['start'],
                            end_time=gap['end']
                        )
                        
                        if missing_data and len(missing_data) > 0:
                            self.logger.info(
                                f"{Fore.GREEN}Fetched {len(missing_data)} candles to fill gap from "
                                f"{self._timestamp_to_datetime(missing_data[0]['timestamp'])} to "
                                f"{self._timestamp_to_datetime(missing_data[-1]['timestamp'])}{Style.RESET_ALL}"
                            )
                            
                            # Add exchange information if not present
                            for candle in missing_data:
                                if 'exchange' not in candle:
                                    candle['exchange'] = self.exchange
                            
                            # Store in database
                            self.candle_service.add_candles(missing_data)
                            gaps_filled += 1
                        else:
                            self.logger.info(f"No data available to fill gap from {gap['start']} to {gap['end']}")
                            
                    except Exception as e:
                        self.logger.error(f"{Fore.RED}Error filling gap for {timeframe}: {e}{Style.RESET_ALL}")
                    
                    # Slight delay to avoid rate limits
                    await asyncio.sleep(0.5)
                
                return gaps_filled
                
            except Exception as e:
                self.logger.error(f"{Fore.RED}Error detecting gaps: {e}{Style.RESET_ALL}")
                return 0

    async def process_real_time_candle(self, candle_data: Dict) -> None:
        """
        Process a real-time candle update.
        Updates the database and in-memory cache.
        
        Args:
            candle_data: Dictionary containing candle data
        """
        try:
            # Ensure we have the exchange information
            if 'exchange' not in candle_data:
                candle_data['exchange'] = self.exchange
                
            timeframe = candle_data.get('timeframe')
            timestamp = candle_data.get('timestamp')
            
            if not timeframe or not timestamp:
                self.logger.error(f"{Fore.RED}Invalid candle data, missing timeframe or timestamp{Style.RESET_ALL}")
                return
                
            # Check if this is a new candle or an update to an existing one
            cache_key = f"{timeframe}_{timestamp}"
            
            if cache_key in self.open_candles[timeframe]:
                # Update existing candle
                existing_candle = self.open_candles[timeframe][cache_key]
                
                # Update relevant fields
                for field in ['open', 'high', 'low', 'close', 'volume']:
                    if field in candle_data:
                        existing_candle[field] = candle_data[field]
                
                # Update the cache
                self.open_candles[timeframe][cache_key] = existing_candle
                
                # Update in database
                self.candle_service.update_candle(existing_candle)
                
                self.logger.debug(f"Updated candle: {self.symbol}/{timeframe} at {self._timestamp_to_datetime(timestamp)}")
            else:
                # New candle
                self.open_candles[timeframe][cache_key] = candle_data
                
                # Add to database
                self.candle_service.add_candles([candle_data])
                
                self.logger.debug(f"New candle: {self.symbol}/{timeframe} at {self._timestamp_to_datetime(timestamp)}")
                
                # Clean up old candles from cache
                await self._clean_candle_cache(timeframe)
                
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error processing real-time candle: {e}{Style.RESET_ALL}")
    
    async def _clean_candle_cache(self, timeframe: str, max_cached: int = 100) -> None:
        """
        Clean up old candles from the cache.
        
        Args:
            timeframe: Timeframe to clean
            max_cached: Maximum number of candles to keep in cache
        """
        try:
            cache = self.open_candles[timeframe]
            
            # If cache is too large, remove oldest candles
            if len(cache) > max_cached:
                # Sort by timestamp
                sorted_keys = sorted(cache.keys(), key=lambda k: cache[k]['timestamp'])
                
                # Remove oldest candles
                keys_to_remove = sorted_keys[:len(cache) - max_cached]
                for key in keys_to_remove:
                    del cache[key]
                
                self.logger.debug(f"Cleaned {len(keys_to_remove)} old candles from {timeframe} cache")
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error cleaning candle cache: {e}{Style.RESET_ALL}")
    
    async def process_batch_candles(self, candles: List[Dict]) -> None:
        """
        Process a batch of candles.
        
        Args:
            candles: List of candle dictionaries
        """
        try:
            if not candles:
                return
                
            # Group candles by timeframe
            timeframe_candles = {}
            for candle in candles:
                timeframe = candle.get('timeframe')
                if timeframe not in timeframe_candles:
                    timeframe_candles[timeframe] = []
                    
                # Ensure we have the exchange information
                if 'exchange' not in candle:
                    candle['exchange'] = self.exchange
                    
                timeframe_candles[timeframe].append(candle)
                
            # Process each timeframe batch
            for timeframe, tf_candles in timeframe_candles.items():
                # Store in database
                self.candle_service.add_candles(tf_candles)
                
                # Update cache
                for candle in tf_candles:
                    timestamp = candle.get('timestamp')
                    cache_key = f"{timeframe}_{timestamp}"
                    self.open_candles[timeframe][cache_key] = candle
                
                self.logger.info(f"Processed batch of {len(tf_candles)} candles for {timeframe}")
                
                # Clean up cache
                await self._clean_candle_cache(timeframe)
                
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error processing batch candles: {e}{Style.RESET_ALL}")
    
    async def run_maintenance(self, interval: int = 3600) -> None:
        """
        Run maintenance tasks periodically.
        - Detect and fill gaps
        - Clean up candle cache
        
        Args:
            interval: Interval in seconds between maintenance runs
        """
        while True:
            try:
                self.logger.info(f"{Fore.BLUE}Running maintenance tasks...{Style.RESET_ALL}")
                
                # Detect and fill gaps for each timeframe
                for timeframe in self.timeframes:
                    gaps_filled = await self.detect_and_fill_gaps(timeframe)
                    self.logger.info(f"Filled {gaps_filled} gaps for {timeframe}")
                    
                    # Clean up cache
                    await self._clean_candle_cache(timeframe)
                    
                self.logger.info(f"{Fore.GREEN}Maintenance tasks completed!{Style.RESET_ALL}")
                
            except Exception as e:
                self.logger.error(f"{Fore.RED}Error during maintenance: {e}{Style.RESET_ALL}")
                
            # Wait for next maintenance run
            await asyncio.sleep(interval)
    
    async def start(self, lookback_days: int = 30) -> None:
        """
        Start the candle database manager.
        
        Args:
            lookback_days: Number of days to look back for historical data
        """
        try:
            # First, populate historical data
            await self.populate_historical_data(lookback_days)
            
            # Start maintenance tasks
            asyncio.create_task(self.run_maintenance())
            
            self.logger.info(f"{Fore.GREEN}CandleDBManager started successfully!{Style.RESET_ALL}")
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error starting CandleDBManager: {e}{Style.RESET_ALL}")
    
    async def get_candles_as_dataframe(self, timeframe: str, limit: int = 1000) -> Optional[pd.DataFrame]:
        """
        Get candles as a pandas DataFrame.
        
        Args:
            timeframe: Timeframe to retrieve
            limit: Maximum number of candles to retrieve
            
        Returns:
            DataFrame or None: Candles as DataFrame or None if error
        """
        try:
            candles = self.candle_service.get_candles(
                self.symbol, self.exchange, timeframe, limit=limit
            )
            
            if not candles:
                return None
                
            # Convert to dicts if they're ORM objects
            candles_data = []
            for c in candles:
                if hasattr(c, '__dict__'):
                    candles_data.append({k: v for k, v in c.__dict__.items() if not k.startswith('_')})
                else:
                    candles_data.append(c)
                    
            # Create DataFrame
            df = pd.DataFrame(candles_data)
            
            # Ensure timestamp is datetime
            if 'timestamp' in df.columns:
                df['datetime'] = df['timestamp'].apply(self._timestamp_to_datetime)
                df = df.sort_values('timestamp')
                
            return df
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error getting candles as DataFrame: {e}{Style.RESET_ALL}")
            return None
    
    async def get_latest_candles(self, timeframe: str, count: int = 1) -> List[Dict]:
        """
        Get the latest candles for a timeframe.
        
        Args:
            timeframe: Timeframe to retrieve
            count: Number of most recent candles to retrieve
            
        Returns:
            List[Dict]: List of latest candles
        """
        try:
            candles = self.candle_service.get_candles(
                self.symbol, self.exchange, timeframe, limit=count
            )
            
            if not candles:
                return []
                
            # Convert to dicts if they're ORM objects
            candles_data = []
            for c in candles:
                if hasattr(c, '__dict__'):
                    candles_data.append({k: v for k, v in c.__dict__.items() if not k.startswith('_')})
                else:
                    candles_data.append(c)
                    
            return candles_data
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error getting latest candles: {e}{Style.RESET_ALL}")
            return []
    
    """ def export_candles_to_csv(self, timeframe: str, filename: str = None, limit: int = None) -> str:
        
        Export candles to a CSV file.
        
        Args:
            timeframe: Timeframe to export
            filename: Output filename (default: {symbol}_{timeframe}_{timestamp}.csv)
            limit: Maximum number of candles to export
            
        Returns:
            str: Path to the exported CSV file
        
        try:
            if not filename:
                timestamp = int(time.time())
                filename = f"{self.symbol}_{timeframe}_{timestamp}.csv"
                
            # Get candles
            candles = self.candle_service.get_candles(
                self.symbol, self.exchange, timeframe, limit=limit
            )
            
            if not candles:
                self.logger.warning(f"No candles found for {self.symbol}/{timeframe}")
                return ""
                
            # Convert to dicts if they're ORM objects
            candles_data = []
            for c in candles:
                if hasattr(c, '__dict__'):
                    candles_data.append({k: v for k, v in c.__dict__.items() if not k.startswith('_')})
                else:
                    candles_data.append(c)
                    
            # Create DataFrame
            df = pd.DataFrame(candles_data)
            
            # Add datetime column
            if 'timestamp' in df.columns:
                df['datetime'] = df['timestamp'].apply(self._timestamp_to_datetime)
                
            # Export to CSV
            df.to_csv(filename, index=False)
            
            self.logger.info(f"{Fore.GREEN}Exported {len(df)} candles to {filename}{Style.RESET_ALL}")
            return filename
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error exporting candles to CSV: {e}{Style.RESET_ALL}")
            return ""
    
    async def import_candles_from_csv(self, filename: str, timeframe: str = None) -> int:
        
        Import candles from a CSV file.
        
        Args:
            filename: Path to CSV file
            timeframe: Timeframe to import (if not specified in CSV)
            
        Returns:
            int: Number of candles imported
        
        try:
            # Read CSV file
            df = pd.read_csv(filename)
            
            # Ensure required columns exist
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            for col in required_cols:
                if col not in df.columns:
                    self.logger.error(f"{Fore.RED}Missing required column: {col}{Style.RESET_ALL}")
                    return 0
                    
            # Convert DataFrame to list of dictionaries
            candles = df.to_dict('records')
            
            # Add missing fields
            for candle in candles:
                if 'exchange' not in candle:
                    candle['exchange'] = self.exchange
                if 'symbol' not in candle:
                    candle['symbol'] = self.symbol
                if 'timeframe' not in candle and timeframe:
                    candle['timeframe'] = timeframe
                    
            # Import candles
            self.candle_service.add_candles(candles)
            
            self.logger.info(f"{Fore.GREEN}Imported {len(candles)} candles from {filename}{Style.RESET_ALL}")
            return len(candles)
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error importing candles from CSV: {e}{Style.RESET_ALL}")
            return 0 """
    
    async def close(self) -> None:
        """Clean up resources and close the manager."""
        try:
            self.logger.info(f"{Fore.YELLOW}Closing CandleDBManager...{Style.RESET_ALL}")
            
            # Close any open connections or resources
            # This is implementation-dependent
            
            self.logger.info(f"{Fore.GREEN}CandleDBManager closed successfully!{Style.RESET_ALL}")
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error closing CandleDBManager: {e}{Style.RESET_ALL}")