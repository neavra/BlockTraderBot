import asyncio
import time
import random
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock

from data.managers.candle_manager import CandleManager
from shared.domain.dto.candle_dto import CandleDto


async def generate_realistic_candle_data(count=1000):
    """Generate realistic candle data in Binance API format with proper price movements."""
    candles = []
    
    # Start with some base values
    timestamp = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
    price = 30000.0  # Starting price for BTC
    
    for i in range(count):
        # Create some realistic price movement
        price_change = price * random.uniform(-0.01, 0.01)  # -1% to +1% change
        new_price = price + price_change
        
        high = max(price, new_price) + random.uniform(0, price * 0.005)  # Add a wick
        low = min(price, new_price) - random.uniform(0, price * 0.005)  # Add a wick
        
        # Create in Binance REST API kline format (which is an array, not an object)
        # Format: [Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, Number of trades, 
        #          Taker buy base volume, Taker buy quote volume, Ignore]
        candle_data = [
            int(timestamp.timestamp() * 1000),                    # Open time
            str(price),                                          # Open
            str(high),                                           # High
            str(low),                                            # Low
            str(new_price),                                      # Close
            str(random.uniform(10, 100)),                        # Volume
            int((timestamp + timedelta(hours=1)).timestamp() * 1000),  # Close time
            str(random.uniform(300000, 3000000)),                # Quote asset volume
            int(random.uniform(1000, 10000)),                    # Number of trades
            str(random.uniform(5, 50)),                          # Taker buy base asset volume
            str(random.uniform(150000, 1500000)),                # Taker buy quote asset volume
            "0"                                                  # Ignore
        ]
        
        candles.append(candle_data)
        
        # Update for next iteration
        timestamp += timedelta(hours=1)
        price = new_price
    
    return candles


# We're not creating mock DTOs anymore since we'll use the real normalizer


async def run_performance_test():
    """Run a performance test on CandleManager.handle_rest_data."""
    print("Starting performance test...")
    
    # Create configuration
    mock_config = {
        'data': {
            'custom_timeframes': {
                'enabled': False
            },
            'exchange': 'binance'
        }
    }
    
    # Create the manager with all dependencies mocked
    mock_database = MagicMock()
    manager = CandleManager(database=mock_database, config=mock_config)
    
    # Mock the producer queues
    manager.producer_candle_queue = MagicMock()
    manager.producer_candle_queue.publish = AsyncMock()
    manager.producer_event_queue = MagicMock()
    manager.producer_event_queue.publish = AsyncMock()
    
    # Mock the candle cache
    manager.candle_cache = MagicMock()
    manager.candle_cache.add_to_sorted_set = AsyncMock()
    
    # Generate realistic candle data
    candle_data = await generate_realistic_candle_data(1000)
    
    # The actual exchange, symbol, and interval to use
    exchange = 'binance'
    symbol = 'BTCUSDT'
    interval = '1h'

    # Time the execution
    start_time = time.time()
    result = await manager.handle_rest_data(candle_data, exchange, symbol, interval)
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    
    # Log the performance results
    print("\nPerformance Test Results:")
    print(f"Time to process 1000 candles: {elapsed_time:.4f} seconds")
    print(f"Average time per candle: {(elapsed_time/1000)*1000:.4f} ms")
    print(f"Candles processed per second: {1000/elapsed_time:.2f}")
    
    # Verify results
    print(f"\nVerification:")
    print(f"Candles received: {len(result)}")
    print(f"Queue publish calls: {manager.producer_candle_queue.publish.call_count}")
    print(f"Event publish calls: {manager.producer_event_queue.publish.call_count}")


if __name__ == '__main__':
    asyncio.run(run_performance_test())