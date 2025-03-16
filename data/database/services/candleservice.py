import logging
import time
from datetime import datetime
from connectors.types import CandleSchema
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import select
from ..models.candlestick import Candlestick 
from ..db import DBAdapter
from typing import Optional, List, Dict, Any
import colorama
from colorama import Fore, Style
import asyncio

# Initialize colorama
colorama.init(autoreset=True)

class CandleService:
    def __init__(self, db_adapter: DBAdapter, log_level=logging.INFO):
        self.db_adapter = db_adapter
        self.setup_logger(log_level)
        self.write_lock = asyncio.Lock()
        self.logger.info(f"{Fore.GREEN}CandleService initialized{Style.RESET_ALL}")
        
    def setup_logger(self, log_level):
        """Set up the logger for the service"""
        self.logger = logging.getLogger("CandleService")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f'{Fore.CYAN}[%(asctime)s]{Style.RESET_ALL} {Fore.MAGENTA}%(levelname)s{Style.RESET_ALL} - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(log_level)
    
    def _format_candle_info(self, candle_data: Dict[str, Any]) -> str:
        """Format candle information for logging"""
        symbol = candle_data.get("symbol", "unknown")
        exchange = candle_data.get("exchange", "unknown")
        timeframe = candle_data.get("timeframe", "unknown")
        timestamp = candle_data.get("timestamp")
        
        if timestamp:
            if isinstance(timestamp, (int, float)):
                timestamp_str = datetime.fromtimestamp(timestamp/1000 if timestamp > 1e10 else timestamp).strftime('%Y-%m-%d %H:%M:%S')
            else:
                timestamp_str = str(timestamp)
        else:
            timestamp_str = "unknown"
            
        return f"{symbol}/{exchange}/{timeframe} @ {timestamp_str}"

    async def add_candle(self, candle_data: CandleSchema) -> Optional[Candlestick]:
        """Add a new candle to the database. TODO FIX THIS METHOD IDK WHY THIS DOESNT WORK BUT ADD CANDLES IS FINE"""
        start_time = time.time()
        candle_info = self._format_candle_info(candle_data.dict() if hasattr(candle_data, "dict") else candle_data.model_dump())
        
        self.logger.info(f"Adding candle: {candle_info}")
        # Use the lock for this database write operation
        self.logger.info(f"Attempting to acquire lock for adding candle: {candle_info}")
        async with self.write_lock:
            self.logger.info(f"Lock acquired for adding candle: {candle_info}")
            # Create a fresh session for this operation
            session = await self.db_adapter.create_async_session()
            try:
                # Start a transaction explicitly
                async with session.begin():
                    candle = Candlestick(**candle_data.dict())
                    session.add(candle)
                    # No need for explicit commit with session.begin()
                
                # Refresh after the transaction is complete
                await session.refresh(candle)    
                elapsed = (time.time() - start_time) * 1000
                self.logger.info(f"{Fore.GREEN}✓ Added candle{Style.RESET_ALL}: {candle_info} ({elapsed:.2f}ms)")
                return candle
            except IntegrityError:
                await session.rollback()
                self.logger.warning(f"{Fore.YELLOW}⚠ Duplicate candle{Style.RESET_ALL}: {candle_info}")
                return None
            except SQLAlchemyError as e:
                await session.rollback()
                self.logger.error(f"{Fore.RED}✗ Error adding candle{Style.RESET_ALL}: {candle_info} - {str(e)}")
                return None
            finally:
                await session.close()
                self.logger.info(f"Lock released after adding candle: {candle_info}")
            
    async def add_candles(self, candles_data: List[CandleSchema]) -> None:
        """Add multiple candles to the database efficiently."""
        if not candles_data:
            self.logger.warning(f"{Fore.YELLOW}⚠ No candles to add{Style.RESET_ALL}")
            return
            
        start_time = time.time()
        sample_candle = candles_data[0]
        #print(sample_candle)
        symbol = sample_candle.symbol
        exchange = sample_candle.exchange
        timeframe = sample_candle.timeframe
        
        self.logger.debug(f"Bulk adding {len(candles_data)} candles for {symbol}/{exchange}/{timeframe}")
        # Use the lock for this database write operation
        async with self.write_lock:
            # Create a fresh session for this operation
            session = await self.db_adapter.create_async_session()
            
            try:
                candles = [Candlestick(**data.dict()) for data in candles_data]
                session.add_all(candles)  # Bulk insert
                await session.commit()
                
                elapsed = (time.time() - start_time) * 1000
                self.logger.info(
                    f"{Fore.GREEN}✓ Added {len(candles_data)} candles{Style.RESET_ALL}: "
                    f"{symbol}/{exchange}/{timeframe} ({elapsed:.2f}ms, {elapsed/len(candles_data):.2f}ms per candle)"
                )
            except IntegrityError:
                self.logger.warning(
                            f"{Fore.YELLOW}⚠ Duplicate entries detected in bulk add{Style.RESET_ALL}: "
                            f"{symbol}/{exchange}/{timeframe} - Rolling back and Adding candles manually..."
                        )
                await session.rollback()
                for candle in candles:
                    try:
                        session.add(candle)  # Try adding one by one
                        await session.commit()
                    except IntegrityError:
                        await session.rollback()  # Rollback only the failing object
                        self.logger.warning(
                            f"{Fore.YELLOW}⚠ Duplicate candle detected{Style.RESET_ALL}: "
                            f"{symbol}/{exchange}/{timeframe}/{candle.timestamp} - transaction rolled back"
                        )
            except SQLAlchemyError as e:
                await session.rollback()
                self.logger.error(
                    f"{Fore.RED}✗ Error in bulk add{Style.RESET_ALL}: "
                    f"{symbol}/{exchange}/{timeframe} - {str(e)} - Rolling back and Adding candles manually..."
                )
                for candle in candles:
                    try:
                        session.add(candle)  # Try adding one by one
                        await session.commit()
                    except SQLAlchemyError as e:
                        await session.rollback()  # Rollback only the failing object
                        self.logger.error(
                            f"{Fore.RED}✗ Error in bulk add{Style.RESET_ALL}: "
                            f"{symbol}/{exchange}/{timeframe} - {str(e)}"
                        )
            finally:
                await session.close()

    async def get_candle(self, symbol: str, exchange: str, timeframe: str, timestamp) -> Optional[Candlestick]:
        """Retrieve a specific candle from the database."""
        start_time = time.time()
        # Create a fresh session for this operation
        session = await self.db_adapter.create_async_session()
        try:
            stmt = select(Candlestick).filter_by(
                symbol=symbol, exchange=exchange, timeframe=timeframe, timestamp=timestamp
            )
            result = await session.execute(stmt)
            candle = result.scalars().first()
            
            elapsed = (time.time() - start_time) * 1000
            if candle:
                self.logger.debug(
                    f"{Fore.BLUE}⚡ Retrieved candle{Style.RESET_ALL}: "
                    f"{symbol}/{exchange}/{timeframe} @ {timestamp} ({elapsed:.2f}ms)"
                )
            else:
                self.logger.debug(
                    f"{Fore.YELLOW}⚠ Candle not found{Style.RESET_ALL}: "
                    f"{symbol}/{exchange}/{timeframe} @ {timestamp} ({elapsed:.2f}ms)"
                )
            return candle
        except SQLAlchemyError as e:
            self.logger.error(
                f"{Fore.RED}✗ Error retrieving candle{Style.RESET_ALL}: "
                f"{symbol}/{exchange}/{timeframe} @ {timestamp} - {str(e)}"
            )
            return None
        finally:
            await session.close()

    async def get_candles(self, symbol: str, exchange: str, timeframe: str, limit: int = 100) -> List[Candlestick]:
        """Retrieve a list of recent candles for a given symbol and timeframe."""
        start_time = time.time()
        # Create a fresh session for this operation
        session = await self.db_adapter.create_async_session()
        
        try:
            stmt = (
                select(Candlestick)
                .filter_by(symbol=symbol, exchange=exchange, timeframe=timeframe)
                .order_by(Candlestick.timestamp.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            candles = result.scalars().all()
            
            elapsed = (time.time() - start_time) * 1000
            self.logger.info(
                f"{Fore.BLUE}⚡ Retrieved {len(candles)} candles{Style.RESET_ALL}: "
                f"{symbol}/{exchange}/{timeframe} (limit={limit}, {elapsed:.2f}ms)"
            )
            return candles
        except SQLAlchemyError as e:
            self.logger.error(
                f"{Fore.RED}✗ Error retrieving candles{Style.RESET_ALL}: "
                f"{symbol}/{exchange}/{timeframe} - {str(e)}"
            )
            return []
        finally:
            await session.close()
    async def get_latest_candle(self, symbol: str, exchange: str, timeframe: str) -> Optional[Dict]:
        """
        Get the most recent candle for a timeframe.
        
        Args:
            timeframe: Timeframe to check
            
        Returns:
            Dict or None: Latest candle data or None if no candles exist
        """
        candles = await self.get_candles(
            symbol, exchange, timeframe, limit=1
        )
        
        if candles and len(candles) > 0:
            # Convert to dict if it's an ORM object
            if hasattr(candles[0], '__dict__'):
                return {k: v for k, v in candles[0].__dict__.items() if not k.startswith('_')}
            return candles[0]
        return None
    
    async def get_earliest_candle(self, symbol: str, exchange: str, timeframe: str) -> Optional[Dict]:
        """
        Get the earliest candle for a timeframe.
        
        Args:
            timeframe: Timeframe to check
            
        Returns:
            Dict or None: Earliest candle data or None if no candles exist
        """
        # Create a fresh session for this operation
        session = await self.db_adapter.create_async_session()
        try:
            # Query the earliest candle by ordering by timestamp ascending
            stmt = (
                select(Candlestick)
                .filter_by(symbol=symbol, exchange=exchange, timeframe=timeframe)
                .order_by(Candlestick.timestamp.asc())
            )
            result = await session.execute(stmt)
            earliest = result.scalars().first()
            
            if earliest:
                # Convert to dict if it's an ORM object
                if hasattr(earliest, '__dict__'):
                    return {k: v for k, v in earliest.__dict__.items() if not k.startswith('_')}
                return earliest
            return None
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error getting earliest candle: {e}{Style.RESET_ALL}")
            return None
        finally:
            await session.close()

    async def update_candle(self, candle_data: dict) -> Optional[Candlestick]:
        """Update an existing candle."""
        start_time = time.time()
        candle_info = self._format_candle_info(candle_data)
        
        self.logger.debug(f"Updating candle: {candle_info}")
        # Use the lock for this database write operation
        async with self.write_lock:
            # Create a fresh session for this operation
            session = await self.db_adapter.create_async_session()

            try:
                stmt = select(Candlestick).filter_by(
                    symbol=candle_data["symbol"],
                    exchange=candle_data["exchange"],
                    timeframe=candle_data["timeframe"], 
                    timestamp=candle_data["timestamp"]
                )
                result = await session.execute(stmt)
                candle = result.scalars().first()
                
                if candle:
                    for key, value in candle_data.items():
                        setattr(candle, key, value)
                    await session.commit()
                    await session.refresh(candle)
                    
                    elapsed = (time.time() - start_time) * 1000
                    self.logger.info(f"{Fore.GREEN}✓ Updated candle{Style.RESET_ALL}: {candle_info} ({elapsed:.2f}ms)")
                    return candle
                else:
                    self.logger.warning(f"{Fore.YELLOW}⚠ Candle not found for update{Style.RESET_ALL}: {candle_info}")
                    return None
            except SQLAlchemyError as e:
                await session.rollback()
                self.logger.error(f"{Fore.RED}✗ Error updating candle{Style.RESET_ALL}: {candle_info} - {str(e)}")
                return None
            finally:
                await session.close()

    async def delete_candle(self, symbol: str, exchange: str, timeframe: str, timestamp) -> bool:
        """Delete a specific candle."""
        start_time = time.time()
        timestamp_str = datetime.fromtimestamp(timestamp/1000 if timestamp > 1e10 else timestamp).strftime('%Y-%m-%d %H:%M:%S') if isinstance(timestamp, (int, float)) else str(timestamp)
        candle_info = f"{symbol}/{exchange}/{timeframe} @ {timestamp_str}"
        
        self.logger.debug(f"Deleting candle: {candle_info}")
        # Use the lock for this database write operation
        async with self.write_lock:
            # Create a fresh session for this operation
            session = await self.db_adapter.create_async_session()
            try:
                stmt = select(Candlestick).filter_by(
                    symbol=symbol, exchange=exchange, timeframe=timeframe, timestamp=timestamp
                )
                result = await session.execute(stmt)
                candle = result.scalars().first()
                
                if candle:
                    session.delete(candle)
                    await session.commit()
                    
                    elapsed = (time.time() - start_time) * 1000
                    self.logger.info(f"{Fore.GREEN}✓ Deleted candle{Style.RESET_ALL}: {candle_info} ({elapsed:.2f}ms)")
                    return True
                else:
                    self.logger.warning(f"{Fore.YELLOW}⚠ Candle not found for deletion{Style.RESET_ALL}: {candle_info}")
                    return False
            except SQLAlchemyError as e:
                await session.rollback()
                self.logger.error(f"{Fore.RED}✗ Error deleting candle{Style.RESET_ALL}: {candle_info} - {str(e)}")
                return False
            finally:
                await session.close()
    
    async def check_for_gaps(self, symbol: str, exchange: str, timeframe: str, max_gaps: int = 100) -> List[tuple]:
        """
        Check for gaps in candle data for a specific symbol, exchange, and timeframe.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT').
            exchange: Exchange name (e.g., 'Binance').
            timeframe: Timeframe to check (e.g., '1m', '1h').
            max_gaps: Maximum number of gaps to return.
            
        Returns:
            List of tuples (gap_start_timestamp, gap_end_timestamp) where each tuple represents a gap.
        """
        start_time = time.time()
        self.logger.debug(f"Checking for gaps in {symbol}/{exchange}/{timeframe}")
        
        # Create a fresh session for this operation
        session = await self.db_adapter.create_async_session()
        try:
            # Get all candles for the symbol/exchange/timeframe, ordered by timestamp
            stmt = (
                select(Candlestick)
                .filter_by(symbol=symbol, exchange=exchange, timeframe=timeframe)
                .order_by(Candlestick.timestamp.asc())
            )
            result = await session.execute(stmt)
            candles = result.scalars().all()
            
            if not candles or len(candles) < 2:
                self.logger.info(
                    f"{Fore.YELLOW}⚠ Not enough candles to check for gaps{Style.RESET_ALL}: "
                    f"{symbol}/{exchange}/{timeframe}"
                )
                return []
            
            # Calculate the expected time interval between candles based on timeframe
            interval_ms = self._get_timeframe_ms(timeframe)
            
            # Find gaps
            gaps = []
            for i in range(1, len(candles)):
                current_timestamp = candles[i].timestamp
                previous_timestamp = candles[i-1].timestamp
                
                # Calculate expected next timestamp
                expected_timestamp = previous_timestamp + interval_ms
                
                # If there's a gap larger than the expected interval
                if (current_timestamp - expected_timestamp) >= interval_ms:
                    # Gap detected
                    gap_start = expected_timestamp
                    gap_end = current_timestamp - interval_ms
                    gaps.append((gap_start, gap_end))
                    
                    self.logger.debug(
                        f"{Fore.YELLOW}⚠ Gap detected{Style.RESET_ALL}: "
                        f"{symbol}/{exchange}/{timeframe} from "
                        f"{datetime.fromtimestamp(gap_start/1000).strftime('%Y-%m-%d %H:%M:%S')} to "
                        f"{datetime.fromtimestamp(gap_end/1000).strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    
                    # Limit the number of gaps to return
                    if len(gaps) >= max_gaps:
                        break
            
            elapsed = (time.time() - start_time) * 1000
            self.logger.info(
                f"{Fore.BLUE}⚡ Found {len(gaps)} gaps{Style.RESET_ALL}: "
                f"{symbol}/{exchange}/{timeframe} ({elapsed:.2f}ms)"
            )
            return gaps
            
        except SQLAlchemyError as e:
            self.logger.error(
                f"{Fore.RED}✗ Error checking for gaps{Style.RESET_ALL}: "
                f"{symbol}/{exchange}/{timeframe} - {str(e)}"
            )
            return []
        finally:
            await session.close()
        
    def _get_timeframe_ms(self, timeframe: str) -> int:
        """
        Converts a timeframe string to milliseconds.
        
        Examples:
            "1m" -> 60000
            "1h" -> 3600000
        """
        unit = timeframe[-1].lower()
        value = int(timeframe[:-1])
        
        if unit == 'm':
            return value * 60 * 1000
        elif unit == 'h':
            return value * 60 * 60 * 1000
        elif unit == 'd':
            return value * 24 * 60 * 60 * 1000
        elif unit == 'w':
            return value * 7 * 24 * 60 * 60 * 1000
        elif unit == 'M':  # Month (approximated)
            return value * 30 * 24 * 60 * 60 * 1000
        else:
            self.logger.error(f"{Fore.RED}Unsupported timeframe unit: {unit}{Style.RESET_ALL}")
            raise ValueError(f"Unsupported timeframe unit: {unit}")