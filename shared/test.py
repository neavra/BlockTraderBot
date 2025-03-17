# shared/main.py - Test script for Cache and Queue services
import time
import logging
import threading
import uuid
import json
from datetime import datetime

# Import services
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges, Queues, RoutingKeys, CacheKeys, CacheTTL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ServiceA:
    """Simulates a data producer service (e.g., Market Data Provider)"""
    
    def __init__(self, queue_service, cache_service):
        self.queue_service = queue_service
        self.cache_service = cache_service
        self.running = False
        self.thread = None
        
        # Setup exchanges and queues
        self.queue_service.declare_exchange(Exchanges.MARKET_DATA)
        logger.info("ServiceA initialized")
    
    def start(self):
        """Start the service in a separate thread"""
        self.running = True
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()
        logger.info("ServiceA started")
    
    def stop(self):
        """Stop the service"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
        logger.info("ServiceA stopped")
    
    def _run(self):
        """Main service loop - generates and publishes data"""
        counter = 0
        
        while self.running:
            counter += 1
            
            # Generate a sample data message
            message_id = str(uuid.uuid4())
            timestamp = time.time()
            
            data = {
                "id": message_id,
                "type": "sample_data",
                "timestamp": timestamp,
                "counter": counter,
                "value": 100 + (counter % 10),
                "source": "ServiceA"
            }
            
            # Store in cache
            cache_key = f"data:sample:{message_id}"
            self.cache_service.set(cache_key, data, expiry=CacheTTL.HOUR)
            
            # Track latest data ID in cache
            self.cache_service.set("serviceA:latest_data_id", message_id)
            
            # Add to history using sorted set
            self.cache_service.add_to_sorted_set(
                "serviceA:data_history", 
                message_id, 
                timestamp
            )
            
            # Publish to queue
            self.queue_service.publish(
                Exchanges.MARKET_DATA,
                "data.sample",
                data
            )
            
            logger.info(f"ServiceA published data #{counter} with ID {message_id}")
            
            # Wait before next message
            time.sleep(1.5)

class ServiceB:
    """Simulates a data consumer service (e.g., Strategy Service)"""
    
    def __init__(self, consumer_queue, publisher_queue, cache_service):
        self.consumer_queue = consumer_queue
        self.publisher_queue = publisher_queue
        self.cache_service = cache_service
        self.processed_data = []
        
        # self.consumer_queue.declare_exchange(Exchanges.STRATEGY)

        # Setup queue for receiving data using the consumer connection
        self.consumer_queue.declare_queue(Queues.TEST_DATA)
        self.consumer_queue.bind_queue(
            Exchanges.MARKET_DATA,
            Queues.TEST_DATA,
            "data.#"  # Subscribe to all data events
        )
        
        # Subscribe to the queue
        self.consumer_queue.subscribe(
            Queues.TEST_DATA,
            self.on_data_received
        )
        
        logger.info("ServiceB initialized and subscribed to data queue")
    
    def on_data_received(self, data):
        """Handle incoming data messages"""
        logger.info(f"ServiceB received data: {data['id']} (#{data['counter']})")
        
        # Process the data
        result = {
            "original_id": data["id"],
            "processed_at": time.time(),
            "original_value": data["value"],
            "processed_value": data["value"] * 2,  # Simple transformation
            "processing_service": "ServiceB"
        }
        
        # Store the processing result in cache
        result_key = f"result:sample:{data['id']}"
        self.cache_service.set(result_key, result, expiry=CacheTTL.HOUR)
        
        # Add to processed history using hash
        self.cache_service.hash_set(
            "serviceB:processed_data",
            data["id"],
            result
        )
        
        # Track this result
        self.processed_data.append(result)
        
        # Publish the result to another exchange
        self.publisher_queue.publish(
            Exchanges.STRATEGY,
            "result.processed",
            result
        )
        
        logger.info(f"ServiceB processed data {data['id']} with result: {result['processed_value']}")

class ServiceC:
    """Simulates a secondary consumer (e.g., Execution Service)"""
    
    def __init__(self, queue_service, cache_service):
        self.queue_service = queue_service
        self.cache_service = cache_service

        self.queue_service.declare_exchange(Exchanges.STRATEGY)

        # Setup queue for receiving processed results
        self.queue_service.declare_queue(Queues.TEST_RESULTS)
        self.queue_service.bind_queue(
            Exchanges.STRATEGY,
            Queues.TEST_RESULTS,
            "result.#"  # Subscribe to all result events
        )
        
        # Subscribe to the queue
        self.queue_service.subscribe(
            Queues.TEST_RESULTS,
            self.on_result_received
        )
        
        logger.info("ServiceC initialized and subscribed to results queue")
    
    def on_result_received(self, result):
        """Handle incoming result messages"""
        logger.info(f"ServiceC received result for data ID: {result['original_id']}")
        
        # Look up the original data from cache
        original_key = f"data:sample:{result['original_id']}"
        original_data = self.cache_service.get(original_key)
        
        if original_data:
            # Create a final report combining original and processed data
            report = {
                "report_id": str(uuid.uuid4()),
                "timestamp": time.time(),
                "original_data": original_data,
                "processed_result": result,
                "final_value": result["processed_value"] + 10,  # Further processing
                "report_service": "ServiceC"
            }
            
            # Store the report in cache
            report_key = f"report:{report['report_id']}"
            self.cache_service.set(report_key, report, expiry=CacheTTL.DAY)
            
            # Record in active reports
            self.cache_service.hash_set(
                "serviceC:active_reports",
                report["report_id"],
                {
                    "report_id": report["report_id"],
                    "created_at": time.time(),
                    "data_id": original_data["id"],
                    "final_value": report["final_value"]
                }
            )
            
            logger.info(f"ServiceC created report {report['report_id']} with final value: {report['final_value']}")
        else:
            logger.warning(f"Original data not found in cache for ID: {result['original_id']}")

def monitor_services(cache_service):
    """Monitors and displays statistics about the services"""
    while True:
        try:
            # Get statistics
            processed_count = len(cache_service.hash_getall("serviceB:processed_data"))
            reports_count = len(cache_service.hash_getall("serviceC:active_reports"))
            
            # Get the latest data history
            data_history = cache_service.get_from_sorted_set(
                "serviceA:data_history", 
                -5, -1,  # Last 5 items
                desc=True
            )
            
            # Print status
            logger.info("=== SYSTEM STATUS ===")
            logger.info(f"Messages processed: {processed_count}")
            logger.info(f"Reports created: {reports_count}")
            logger.info(f"Latest data IDs: {data_history}")
            logger.info("====================")
            
            time.sleep(5)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error in monitoring: {str(e)}")
            time.sleep(5)

def main():
    """Main function to set up and run the test"""
    try:
        # Add missing constants for testing
        if not hasattr(Queues, 'TEST_DATA'):
            setattr(Queues, 'TEST_DATA', 'test_data_queue')
        
        if not hasattr(Queues, 'TEST_RESULTS'):
            setattr(Queues, 'TEST_RESULTS', 'test_results_queue')
        
        # Initialize services with separate connections
        publisher_queue = QueueService(host='localhost')     # For publishing operations
        consumer_queue_b = QueueService(host='localhost')    # For ServiceB consuming
        consumer_queue_c = QueueService(host='localhost')    # For ServiceC consuming
        cache_service = CacheService(host='localhost')
        
        logger.info("Services initialized successfully")
        
        # Clear any existing test data in cache
        test_keys = cache_service.keys("data:sample:*")
        for key in test_keys:
            cache_service.delete(key)
        
        # Set up exchanges on all connections
        publisher_queue.declare_exchange(Exchanges.MARKET_DATA)
        publisher_queue.declare_exchange(Exchanges.STRATEGY)
        consumer_queue_b.declare_exchange(Exchanges.MARKET_DATA)
        consumer_queue_b.declare_exchange(Exchanges.STRATEGY)
        consumer_queue_c.declare_exchange(Exchanges.MARKET_DATA)
        consumer_queue_c.declare_exchange(Exchanges.STRATEGY)
        
        # Initialize services with appropriate connections
        service_a = ServiceA(publisher_queue, cache_service)  # Producer only
        
        # Pass dedicated connections to each service
        service_b = ServiceB(consumer_queue_b, publisher_queue, cache_service)
        
        # ServiceC gets its own consumer connection
        service_c = ServiceC(consumer_queue_c, cache_service)
        
        # Start the data producer
        service_a.start()
        
        # Start the monitoring thread
        monitor_thread = threading.Thread(target=monitor_services, args=(cache_service,))
        monitor_thread.daemon = True
        monitor_thread.start()
        
        logger.info("Test environment is running. Press Ctrl+C to stop.")
        
        # Keep the main thread running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping test environment...")
        
        # Clean shutdown
        service_a.stop()
        publisher_queue.stop()
        consumer_queue_b.stop()
        consumer_queue_c.stop()
        cache_service.close()
        logger.info("Test environment stopped")
        
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        raise

if __name__ == "__main__":
    main()