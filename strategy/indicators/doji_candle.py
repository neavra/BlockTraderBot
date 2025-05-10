from typing import Dict, Any, List
from datetime import datetime, timezone
from strategy.indicators.base import Indicator
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.dto.doji_dto import DojiDto, DojiResultDto
from data.database.repository.doji_repository import DojiRepository

import logging

logger = logging.getLogger(__name__)

class DojiCandleIndicator(Indicator):
    """
    Indicator that detects Doji candle patterns.
    
    A Doji candle is characterized by having a very small body (open and close prices are nearly equal)
    relative to its range (high and low). It indicates market indecision and potential reversal points.
    """
    
    def __init__(self, repository: DojiRepository, params: Dict[str, Any] = None):
        """
        Initialize Doji candle detector with parameters
        
        Args:
            params: Dictionary containing parameters:
                - max_body_to_range_ratio: Maximum ratio of body to total range to qualify as doji
                - min_range_to_price_ratio: Minimum candle range relative to price for significance
                - lookback_period: Number of candles to analyze
        """
        self.repository = repository

        default_params = {
            'max_body_to_range_ratio': 0.1,     # Maximum body/range ratio to qualify as doji
            'min_range_to_price_ratio': 0.005,  # Minimum range/price ratio (filters out tiny dojis)
            'lookback_period': 20               # Number of candles to look back
        }
        
        if params:
            default_params.update(params)
            
        super().__init__(repository, default_params)
    
    async def calculate(self, data: Dict[str, Any]) -> DojiResultDto:
        """
        Detect Doji candle patterns in the provided data and save them to the database.
        
        Args:
            data: Dictionary containing:
                - candles: List of OHLCV candles
                - symbol: Trading symbol
                - timeframe: Timeframe
                - exchange: Exchange name
                - current_price: Current market price (optional)
                
        Returns:
            DojiResultDto with detected doji patterns
        """
        candles: List[CandleDto] = data.get("candles")
        
        # Extract market data information
        symbol = data.get("symbol")
        timeframe = data.get("timeframe")
        exchange = data.get("exchange", "default")
        
        # Need enough candles to analyze
        if len(candles) < 3:
            logger.warning("Not enough candles to detect doji patterns (minimum 3 required)")
            return DojiResultDto(
                timestamp=datetime.now(timezone.utc),
                indicator_name="Doji",
                dojis=[]
            )
        
        lookback_period = min(self.params['lookback_period'], len(candles))
        dojis = []
        
        # Process each candle in the lookback period (from most recent)
        for i in range(1, lookback_period + 1):
            candle_idx = len(candles) - i
            if candle_idx < 0:
                break
                
            candle = candles[candle_idx]
            
            # Calculate key metrics for doji identification
            body_size = abs(candle.close - candle.open)
            total_range = candle.high - candle.low
            
            # Avoid division by zero
            if total_range == 0:
                continue
                
            body_to_range_ratio = body_size / total_range
            
            # Calculate total wick size
            total_wick_size = total_range - body_size
            
            # Check price-relative size (to filter out insignificant dojis)
            avg_price = (candle.high + candle.low) / 2
            range_to_price_ratio = total_range / avg_price
            
            # Basic doji qualification: small body relative to range and significant range
            if (body_to_range_ratio <= self.params['max_body_to_range_ratio'] and 
                range_to_price_ratio >= self.params['min_range_to_price_ratio']):
                
                # Get timestamp if available
                timestamp = candle.timestamp
                
                # Create doji DTO
                doji = DojiDto(
                    index=candle_idx,
                    body_to_range_ratio=body_to_range_ratio,
                    total_wick_size=total_wick_size,
                    strength=1.0 - body_to_range_ratio,  # Higher strength for smaller bodies
                    candle=candle,
                    timestamp=timestamp
                )
                
                dojis.append(doji)
        
        # Sort dojis by index (most recent first)
        dojis.sort(key=lambda x: x.index, reverse=True)
        
        # Save detected doji patterns to the database
        try:
            if dojis:
                # Prepare the database records
                doji_records = []
                for doji in dojis:
                    # Create the database record
                    doji_data = {
                        "exchange": exchange,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "body_to_range_ratio": float(doji.body_to_range_ratio),
                        "total_wick_size": float(doji.total_wick_size),
                        "strength": float(doji.strength),
                        "candle_index": doji.index,
                        "timestamp": doji.timestamp if isinstance(doji.timestamp, datetime) else 
                                    datetime.fromisoformat(doji.timestamp.replace('Z', '+00:00')),
                        "candle_data": self._serialize_candle(doji.candle),
                        "analyzed_at": datetime.now(timezone.utc),
                        
                        # Add indicator ID (use the correct enum value in your implementation)
                        "indicator_id": 4,  # Assuming 4 is the ID for DOJI_CANDLE in your indicator registry
                        
                        # Add timestamps
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc)
                    }
                    
                    doji_records.append(doji_data)
                
                # Bulk create the records
                if doji_records:
                    created_records = await self.repository.bulk_create_dojis(doji_records)
                    logger.info(f"Saved {len(created_records)} doji patterns to database")
        except Exception as e:
            logger.error(f"Error saving doji patterns to database: {e}")
        
        # Create and return the result DTO
        return DojiResultDto(
            timestamp=datetime.now(timezone.utc),
            indicator_name="Doji",
            dojis=dojis
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
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns data requirements for doji detection
        
        Returns:
            Dictionary with requirements
        """
        return {
            'candles': True,
            'lookback_period': self.params['lookback_period'],
            'timeframes': ['1m', '5m', '15m', '30m', '1h', '4h', '1d'],  # Supported timeframes
            'indicators': []  # No dependency on other indicators
        }