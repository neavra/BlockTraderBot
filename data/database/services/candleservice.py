import logging
import time
from datetime import datetime
from connectors.types import CandleSchema
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from ..models.candlestick import Candlestick 
from ..db import DBAdapter
from typing import Optional, List, Dict, Any
import colorama
from colorama import Fore, Style

# Initialize colorama
colorama.init(autoreset=True)

class CandleService:
    def __init__(self, db_adapter: DBAdapter, log_level=logging.INFO):
        self.db_adapter = db_adapter
        self.setup_logger(log_level)
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

    def add_candle(self, candle_data: dict) -> Optional[Candlestick]:
        """Add a new candle to the database."""
        start_time = time.time()
        candle_info = self._format_candle_info(candle_data)
        
        self.logger.debug(f"Adding candle: {candle_info}")
        
        with self.db_adapter.get_db() as session:
            try:
                candle = Candlestick(**candle_data)
                session.add(candle)
                session.commit()
                session.refresh(candle)
                
                elapsed = (time.time() - start_time) * 1000
                self.logger.info(f"{Fore.GREEN}✓ Added candle{Style.RESET_ALL}: {candle_info} ({elapsed:.2f}ms)")
                return candle
            except IntegrityError:
                session.rollback()
                self.logger.warning(f"{Fore.YELLOW}⚠ Duplicate candle{Style.RESET_ALL}: {candle_info}")
                return None
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"{Fore.RED}✗ Error adding candle{Style.RESET_ALL}: {candle_info} - {str(e)}")
                return None
            
    def add_candles(self, candles_data: List[CandleSchema]) -> None:
        """Add multiple candles to the database efficiently."""
        if not candles_data:
            self.logger.warning(f"{Fore.YELLOW}⚠ No candles to add{Style.RESET_ALL}")
            return
            
        start_time = time.time()
        sample_candle = candles_data[0]
        symbol = sample_candle.symbol
        exchange = sample_candle.exchange
        timeframe = sample_candle.timeframe
        
        self.logger.debug(f"Bulk adding {len(candles_data)} candles for {symbol}/{exchange}/{timeframe}")
        
        with self.db_adapter.get_db() as session:
            try:
                candles = [Candlestick(**data.dict()) for data in candles_data]
                session.bulk_save_objects(candles)  # Bulk insert
                session.commit()
                
                elapsed = (time.time() - start_time) * 1000
                self.logger.info(
                    f"{Fore.GREEN}✓ Added {len(candles_data)} candles{Style.RESET_ALL}: "
                    f"{symbol}/{exchange}/{timeframe} ({elapsed:.2f}ms, {elapsed/len(candles_data):.2f}ms per candle)"
                )
            except IntegrityError:
                session.rollback()
                self.logger.warning(
                    f"{Fore.YELLOW}⚠ Duplicate entries detected in bulk add{Style.RESET_ALL}: "
                    f"{symbol}/{exchange}/{timeframe} - transaction rolled back"
                )
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error(
                    f"{Fore.RED}✗ Error in bulk add{Style.RESET_ALL}: "
                    f"{symbol}/{exchange}/{timeframe} - {str(e)}"
                )

    def get_candle(self, symbol: str, exchange: str, timeframe: str, timestamp) -> Optional[Candlestick]:
        """Retrieve a specific candle from the database."""
        start_time = time.time()
        
        with self.db_adapter.get_db() as session:
            try:
                candle = session.query(Candlestick).filter_by(
                    symbol=symbol, exchange=exchange, timeframe=timeframe, timestamp=timestamp
                ).first()
                
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

    def get_candles(self, symbol: str, exchange: str, timeframe: str, limit: int = 100) -> List[Candlestick]:
        """Retrieve a list of recent candles for a given symbol and timeframe."""
        start_time = time.time()
        
        with self.db_adapter.get_db() as session:
            try:
                candles = (
                    session.query(Candlestick)
                    .filter_by(symbol=symbol, exchange=exchange, timeframe=timeframe)
                    .order_by(Candlestick.timestamp.desc())
                    .limit(limit)
                    .all()
                )
                
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
    def get_latest_candle(self, symbol: str, exchange: str, timeframe: str) -> Optional[Dict]:
        """
        Get the most recent candle for a timeframe.
        
        Args:
            timeframe: Timeframe to check
            
        Returns:
            Dict or None: Latest candle data or None if no candles exist
        """
        candles = self.get_candles(
            symbol, exchange, timeframe, limit=1
        )
        
        if candles and len(candles) > 0:
            # Convert to dict if it's an ORM object
            if hasattr(candles[0], '__dict__'):
                return {k: v for k, v in candles[0].__dict__.items() if not k.startswith('_')}
            return candles[0]
        return None
    
    def get_earliest_candle(self, symbol: str, exchange: str, timeframe: str) -> Optional[Dict]:
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
                earliest = session.query(Candlestick).filter_by(
                    symbol=symbol, exchange=exchange, timeframe=timeframe
                ).order_by(Candlestick.timestamp.asc()).first()
                
                if earliest:
                    # Convert to dict if it's an ORM object
                    if hasattr(earliest, '__dict__'):
                        return {k: v for k, v in earliest.__dict__.items() if not k.startswith('_')}
                    return earliest
                return None
            except Exception as e:
                self.logger.error(f"{Fore.RED}Error getting earliest candle: {e}{Style.RESET_ALL}")
                return None

    def update_candle(self, candle_data: dict) -> Optional[Candlestick]:
        """Update an existing candle."""
        start_time = time.time()
        candle_info = self._format_candle_info(candle_data)
        
        self.logger.debug(f"Updating candle: {candle_info}")
        
        with self.db_adapter.get_db() as session:
            try:
                candle = session.query(Candlestick).filter_by(
                    symbol=candle_data["symbol"],
                    exchange=candle_data["exchange"],
                    timeframe=candle_data["timeframe"], 
                    timestamp=candle_data["timestamp"]
                ).first()
                
                if candle:
                    for key, value in candle_data.items():
                        setattr(candle, key, value)
                    session.commit()
                    session.refresh(candle)
                    
                    elapsed = (time.time() - start_time) * 1000
                    self.logger.info(f"{Fore.GREEN}✓ Updated candle{Style.RESET_ALL}: {candle_info} ({elapsed:.2f}ms)")
                    return candle
                else:
                    self.logger.warning(f"{Fore.YELLOW}⚠ Candle not found for update{Style.RESET_ALL}: {candle_info}")
                    return None
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"{Fore.RED}✗ Error updating candle{Style.RESET_ALL}: {candle_info} - {str(e)}")
                return None

    def delete_candle(self, symbol: str, exchange: str, timeframe: str, timestamp) -> bool:
        """Delete a specific candle."""
        start_time = time.time()
        timestamp_str = datetime.fromtimestamp(timestamp/1000 if timestamp > 1e10 else timestamp).strftime('%Y-%m-%d %H:%M:%S') if isinstance(timestamp, (int, float)) else str(timestamp)
        candle_info = f"{symbol}/{exchange}/{timeframe} @ {timestamp_str}"
        
        self.logger.debug(f"Deleting candle: {candle_info}")
        
        with self.db_adapter.get_db() as session:
            try:
                candle = session.query(Candlestick).filter_by(
                    symbol=symbol, exchange=exchange, timeframe=timeframe, timestamp=timestamp
                ).first()
                
                if candle:
                    session.delete(candle)
                    session.commit()
                    
                    elapsed = (time.time() - start_time) * 1000
                    self.logger.info(f"{Fore.GREEN}✓ Deleted candle{Style.RESET_ALL}: {candle_info} ({elapsed:.2f}ms)")
                    return True
                else:
                    self.logger.warning(f"{Fore.YELLOW}⚠ Candle not found for deletion{Style.RESET_ALL}: {candle_info}")
                    return False
            except SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"{Fore.RED}✗ Error deleting candle{Style.RESET_ALL}: {candle_info} - {str(e)}")
                return False