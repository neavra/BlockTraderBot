import asyncio
import unittest
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from unittest.mock import MagicMock, AsyncMock, patch

# Import the CandleConsumer and its dependencies
from data.consumer.candle_consumer import CandleConsumer
from shared.domain.dto.candle_dto import CandleDto
from data.database.db import Database
from shared.queue.queue_service import QueueService
from data.database.repository.candle_repository import CandleRepository

class TestCandleConsumer(unittest.IsolatedAsyncioTestCase):
    """Test case for benchmarking CandleConsumer performance."""
    
    async def asyncSetUp(self):
        """Set up the test environment."""
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("TestCandleConsumer")

        # Create proper mock for the repository
        self.mock_repository = AsyncMock(spec=CandleRepository)
        # Configure repository mock to always return empty list for find_by_exchange_symbol_timeframe
        self.mock_repository.find_by_exchange_symbol_timeframe.return_value = []
        
        # Setup the repository_patcher to replace CandleRepository with our mock
        self.repository_patcher = patch('data.consumer.candle_consumer.CandleRepository', 
                                        return_value=self.mock_repository)
        # Start the patcher
        self.repository_patcher.start()
        
        # Mock the database
        self.mock_db = MagicMock(spec=Database)
        self.mock_session = MagicMock()
        self.mock_db.get_session.return_value = self.mock_session
        
        # Mock the queue service
        self.mock_queue = MagicMock(spec=QueueService)
        self.queue_patcher = patch('data.consumer.candle_consumer.QueueService', 
                                   return_value=self.mock_queue)
        self.queue_patcher.start()
        
        # Create the consumer with mocked database
        self.consumer = CandleConsumer(database=self.mock_db)
        
        # Store the current event loop
        self.consumer.main_loop = asyncio.get_running_loop()
        
        # Initialize the consumer - this should now use our mocked repository
        await self.consumer.initialize()
        
        # Verify our mock is being used
        self.assertIs(self.consumer.repository, self.mock_repository, 
                     "Mock repository not properly applied to CandleConsumer!")
    
    async def asyncTearDown(self):
        """Clean up after tests."""
        # Stop patchers
        self.repository_patcher.stop()
        self.queue_patcher.stop()
        
    def generate_test_candles(self, count: int = 1000) -> List[Dict[str, Any]]:
        """
        Generate a list of synthetic candle data for testing.
        
        Args:
            count: Number of candles to generate
            
        Returns:
            List of candle dictionaries
        """
        candles = []
        base_time = datetime.now(timezone.utc)
        test_symbol = f"TEST-{int(time.time())}"  # Use unique symbol to avoid conflicts
        
        for i in range(count):
            # Create candle with incrementing timestamp
            timestamp = base_time - timedelta(minutes=i)
            
            # Simulate price movement
            base_price = 50000.0
            open_price = base_price + (i % 100)
            close_price = open_price + ((-1) ** i) * (i % 20)
            high_price = max(open_price, close_price) + (i % 10)
            low_price = min(open_price, close_price) - (i % 5)
            
            candle = {
                "symbol": test_symbol,
                "exchange": "binance",
                "timeframe": "1m",
                "timestamp": timestamp,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": 1000 + (i % 500),
                "is_closed": True
            }
            
            candles.append(candle)
            
        return candles
    
    async def test_process_1000_candles_sequential(self):
        """Test processing 1000 candles sequentially and measure performance."""
        # Generate test candles
        candles = self.generate_test_candles(1000)
        
        # Verify our mock is being used
        self.assertIs(self.consumer.repository, self.mock_repository, 
                     "Mock repository not properly applied to CandleConsumer!")
        
        # Reset mock counters
        self.mock_repository.create.reset_mock()
        self.mock_repository.update.reset_mock()
        self.mock_repository.find_by_exchange_symbol_timeframe.reset_mock()
        
        # Prepare for testing
        self.logger.info(f"Starting test: Processing 1000 candles sequentially")
        
        # Print initial mock status
        self.logger.info(f"Initial mock status - create calls: {self.mock_repository.create.await_count}, find calls: {self.mock_repository.find_by_exchange_symbol_timeframe.await_count}")
        
        # Measure total processing time
        start_time = time.time()
        
        # Process candles one by one
        for i, candle in enumerate(candles):
            await self.consumer.process_item(candle)
            
            # Log progress
            if (i+1) % 200 == 0:
                self.logger.info(f"Processed {i+1} candles, repository calls: {self.mock_repository.find_by_exchange_symbol_timeframe.await_count}")
        
        # Calculate and log metrics
        end_time = time.time()
        total_time = end_time - start_time
        candles_per_second = 1000 / total_time
        
        # Get call counts
        find_count = self.mock_repository.find_by_exchange_symbol_timeframe.await_count
        create_count = self.mock_repository.create.await_count
        update_count = self.mock_repository.update.await_count
        
        self.logger.info(f"Sequential processing complete")
        self.logger.info(f"Repository calls - find: {find_count}, create: {create_count}, update: {update_count}")
        self.logger.info(f"Total time: {total_time:.2f} seconds")
        self.logger.info(f"Average processing rate: {candles_per_second:.2f} candles/second")
        self.logger.info(f"Average time per candle: {(total_time/1000)*1000:.2f} ms")
        
        # Verify repository was called correctly
        self.assertEqual(self.mock_repository.find_by_exchange_symbol_timeframe.call_count, 1000, "Expected 1000 find calls")
        self.assertEqual(self.mock_repository.create.call_count, 1000, "Expected 1000 create calls since find returns empty list")
    
    async def test_process_1000_candles_concurrent(self):
        """Test processing 1000 candles concurrently and measure performance."""
        # Generate test candles
        candles = self.generate_test_candles(1000)
        
        # Reset mock call counts
        self.mock_repository.create.reset_mock()
        self.mock_repository.update.reset_mock()
        self.mock_repository.find_by_exchange_symbol_timeframe.reset_mock()
        
        # Prepare for testing
        self.logger.info(f"Starting test: Processing 1000 candles concurrently")
        
        # Measure total processing time
        start_time = time.time()
        
        # Create tasks for all candles and run them concurrently
        tasks = [self.consumer.process_item(candle) for candle in candles]
        await asyncio.gather(*tasks)
        
        # Calculate and log metrics
        end_time = time.time()
        total_time = end_time - start_time
        candles_per_second = 1000 / total_time
        
        # Log repository call counts for debugging
        create_count = self.mock_repository.create.await_count
        find_count = self.mock_repository.find_by_exchange_symbol_timeframe.await_count
        update_count = self.mock_repository.update.await_count
        
        self.logger.info(f"Concurrent processing complete")
        self.logger.info(f"Repository calls - find: {find_count}, create: {create_count}, update: {update_count}")
        self.logger.info(f"Total time: {total_time:.2f} seconds")
        self.logger.info(f"Average processing rate: {candles_per_second:.2f} candles/second")
        self.logger.info(f"Average time per candle: {(total_time/1000)*1000:.2f} ms")
        
        # Verify all candles were processed (all should trigger find operation)
        self.assertEqual(
            self.mock_repository.find_by_exchange_symbol_timeframe.call_count, 
            1000
        )
        
        # All candles should either be created or updated
        self.assertEqual(
            self.mock_repository.create.call_count + self.mock_repository.update.call_count, 
            1000
        )
    
    async def test_process_1000_candles_batched(self):
        """Test processing 1000 candles in batches and measure performance."""
        # Generate test candles
        candles = self.generate_test_candles(1000)
        
        # Reset mock call counts
        self.mock_repository.create.reset_mock()
        self.mock_repository.update.reset_mock()
        self.mock_repository.find_by_exchange_symbol_timeframe.reset_mock()
        
        # Prepare for testing
        self.logger.info(f"Starting test: Processing 1000 candles in batches")
        
        # Measure total processing time
        start_time = time.time()
        
        # Process candles in batches of 100
        batch_size = 100
        for i in range(0, 1000, batch_size):
            batch = candles[i:i+batch_size]
            tasks = [self.consumer.process_item(candle) for candle in batch]
            await asyncio.gather(*tasks)
            self.logger.info(f"Processed batch {i//batch_size + 1}/{1000//batch_size}")
        
        # Calculate and log metrics
        end_time = time.time()
        total_time = end_time - start_time
        candles_per_second = 1000 / total_time
        
        # Log repository call counts for debugging
        create_count = self.mock_repository.create.await_count
        find_count = self.mock_repository.find_by_exchange_symbol_timeframe.await_count
        update_count = self.mock_repository.update.await_count
        
        self.logger.info(f"Batched processing complete")
        self.logger.info(f"Repository calls - find: {find_count}, create: {create_count}, update: {update_count}")
        self.logger.info(f"Total time: {total_time:.2f} seconds")
        self.logger.info(f"Average processing rate: {candles_per_second:.2f} candles/second")
        self.logger.info(f"Average time per candle: {(total_time/1000)*1000:.2f} ms")
        
       # Verify all candles were processed (all should trigger find operation)
        self.assertEqual(
            self.mock_repository.find_by_exchange_symbol_timeframe.call_count, 
            1000
        )
        
        # All candles should either be created or updated
        self.assertEqual(
            self.mock_repository.create.call_count + self.mock_repository.update.call_count, 
            1000
        )
    
    async def test_simulate_queue_handling(self):
        """
        Simulate how candles would be processed when coming through the queue.
        Tests the on_candle handler by directly calling it 1000 times.
        """
        # Generate test candles
        candles = self.generate_test_candles(1000)
        
        # Reset mock call counts
        self.mock_repository.create.reset_mock()
        self.mock_repository.update.reset_mock()
        self.mock_repository.find_by_exchange_symbol_timeframe.reset_mock()
        
        # Prepare for testing
        self.logger.info(f"Starting test: Simulating 1000 candles through queue handler")
        
        # Mock run_coroutine_threadsafe to track tasks
        tasks = []
        
        def mock_run_coroutine_threadsafe(coro, loop):
            task = asyncio.create_task(coro)
            tasks.append(task)
            # Make a wrapper Future that acts like the concurrent.futures.Future that
            # would be returned by run_coroutine_threadsafe
            future = MagicMock()
            future.add_done_callback = lambda cb: None
            return future
            
        # Measure the time to process all on_candle calls
        start_time = time.time()
        
        with patch('asyncio.run_coroutine_threadsafe', side_effect=mock_run_coroutine_threadsafe):
            # Send all candles to the on_candle handler
            for i, candle in enumerate(candles):
                self.consumer.on_candle(candle)
                if (i+1) % 100 == 0:
                    self.logger.info(f"Queued {i+1} candles")
                
            # Wait for all tasks to complete
            if tasks:
                self.logger.info(f"Waiting for {len(tasks)} tasks to complete...")
                await asyncio.gather(*tasks)
        
        # Calculate and log metrics
        end_time = time.time()
        total_time = end_time - start_time
        candles_per_second = 1000 / total_time
        
        # Log repository call counts for debugging
        create_count = self.mock_repository.create.await_count
        find_count = self.mock_repository.find_by_exchange_symbol_timeframe.await_count
        update_count = self.mock_repository.update.await_count
        
        self.logger.info(f"Queue simulation complete")
        self.logger.info(f"Repository calls - find: {find_count}, create: {create_count}, update: {update_count}")
        self.logger.info(f"Total time: {total_time:.2f} seconds")
        self.logger.info(f"Average processing rate: {candles_per_second:.2f} candles/second")
        self.logger.info(f"Average time per candle: {(total_time/1000)*1000:.2f} ms")
        self.logger.info(f"Generated tasks: {len(tasks)}")
        
        # Verify all tasks were created
        self.assertEqual(len(tasks), 1000)
        
        # Verify all candles were processed through repository calls
        # Verify all candles were processed (all should trigger find operation)
        self.assertEqual(
            self.mock_repository.find_by_exchange_symbol_timeframe.call_count, 
            1000
        )
        
        # All candles should either be created or updated
        self.assertEqual(
            self.mock_repository.create.call_count + self.mock_repository.update.call_count, 
            1000
        )

    async def test_bulk_processing_simulation(self):
        """
        Simulate high-frequency candle processing with small delays.
        This simulates 1000 candles coming in with 0.1ms gaps between them.
        """
        # Generate test candles
        candles = self.generate_test_candles(1000)
        
        # Reset mock call counts
        self.mock_repository.create.reset_mock()
        self.mock_repository.update.reset_mock()
        self.mock_repository.find_by_exchange_symbol_timeframe.reset_mock()
        
        # Prepare for testing
        self.logger.info(f"Starting test: Simulating high-frequency processing (0.1ms between candles)")
        
        # Track tasks
        tasks = []
        
        # Measure the time to process all candles
        start_time = time.time()
        
        # Process candles with small delays to simulate high-frequency arrival
        for i, candle in enumerate(candles):
            task = asyncio.create_task(self.consumer.process_item(candle))
            tasks.append(task)
            # Simulate 0.1ms delay between candles
            await asyncio.sleep(0.0001)
            
            if (i+1) % 100 == 0:
                self.logger.info(f"Queued {i+1} candles")
        
        # Wait for all tasks to complete
        if tasks:
            self.logger.info(f"Waiting for {len(tasks)} tasks to complete...")
            await asyncio.gather(*tasks)
        
        # Calculate and log metrics
        end_time = time.time()
        total_time = end_time - start_time
        arrival_time = 0.0001 * 999  # Total time just for arrivals
        processing_time = total_time - arrival_time
        candles_per_second = 1000 / processing_time if processing_time > 0 else 0
        
        # Log repository call counts for debugging
        create_count = self.mock_repository.create.await_count
        find_count = self.mock_repository.find_by_exchange_symbol_timeframe.await_count
        update_count = self.mock_repository.update.await_count
        
        self.logger.info(f"High-frequency simulation complete")
        self.logger.info(f"Repository calls - find: {find_count}, create: {create_count}, update: {update_count}")
        self.logger.info(f"Total wall time: {total_time:.2f} seconds")
        self.logger.info(f"Candle arrival time: {arrival_time:.2f} seconds")
        self.logger.info(f"Effective processing time: {processing_time:.2f} seconds")
        self.logger.info(f"Average processing rate: {candles_per_second:.2f} candles/second")
        self.logger.info(f"Average time per candle: {(processing_time/1000)*1000:.2f} ms")
        
        # Verify correct number of repository calls
        # Verify all candles were processed (all should trigger find operation)
        self.assertEqual(
            self.mock_repository.find_by_exchange_symbol_timeframe.call_count, 
            1000
        )
        
        # All candles should either be created or updated
        self.assertEqual(
            self.mock_repository.create.call_count + self.mock_repository.update.call_count, 
            1000
        )

if __name__ == '__main__':
    unittest.main()