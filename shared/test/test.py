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
        
        # Sample exchanges and symbols for testing
        exchanges = ["binance", "coinbase", "ftx"]
        symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]
        timeframes = ["1m", "5m", "15m", "1h", "4h"]
        
        while self.running:
            counter += 1
            
            # Simulate different data sources by cycling through exchanges, symbols, and timeframes
            exchange = exchanges[counter % len(exchanges)]
            symbol = symbols[(counter // 3) % len(symbols)]
            timeframe = timeframes[(counter // 9) % len(timeframes)]
            
            # Generate a sample candle data message
            message_id = str(uuid.uuid4())
            timestamp = time.time()
            
            candle_data = {
                "id": message_id,
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "timestamp": timestamp,
                "open": 100 + (counter % 10),
                "high": 105 + (counter % 10),
                "low": 95 + (counter % 10),
                "close": 102 + (counter % 10),
                "volume": 1000 + (counter * 100)
            }
            
            # Store in cache using updated format
            cache_key = CacheKeys.CANDLE_DATA.format(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=int(timestamp)
            )
            self.cache_service.set(cache_key, candle_data, expiry=CacheTTL.HOUR)
            
            # Update latest candle cache
            latest_key = CacheKeys.LATEST_CANDLE.format(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe
            )
            self.cache_service.set(latest_key, candle_data)
            
            # Add to history using sorted set
            history_key = CacheKeys.CANDLE_HISTORY_SET.format(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe
            )
            self.cache_service.add_to_sorted_set(
                history_key,
                message_id, 
                timestamp
            )
            
            # Publish to queue with updated routing key format
            routing_key = RoutingKeys.CANDLE_NEW.format(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe
            )
            
            self.queue_service.publish(
                Exchanges.MARKET_DATA,
                routing_key,
                candle_data
            )
            
            logger.info(f"ServiceA published {timeframe} candle for {exchange}:{symbol} with ID {message_id}")
            
            # Wait before next message
            time.sleep(1.5)

class ServiceB:
    """Simulates a data consumer service (e.g., Strategy Service)"""
    
    def __init__(self, consumer_queue, publisher_queue, cache_service):
        self.consumer_queue = consumer_queue
        self.publisher_queue = publisher_queue
        self.cache_service = cache_service
        self.processed_data = []
        
        # Setup queue for receiving data using the consumer connection
        self.consumer_queue.declare_queue(Queues.CANDLES)
        
        # Bind with wildcard pattern to catch all candle updates
        self.consumer_queue.bind_queue(
            Exchanges.MARKET_DATA,
            Queues.CANDLES,
            RoutingKeys.CANDLE_ALL  # Subscribe to all candle events
        )
        
        # Subscribe to the queue
        self.consumer_queue.subscribe(
            Queues.CANDLES,
            self.on_candle_received
        )
        
        logger.info("ServiceB initialized and subscribed to candle data queue")
    
    def on_candle_received(self, candle_data):
        """Handle incoming candle data messages"""
        exchange = candle_data["exchange"]
        symbol = candle_data["symbol"]
        timeframe = candle_data["timeframe"]
        
        logger.info(f"ServiceB received {timeframe} candle for {exchange}:{symbol} (ID: {candle_data['id']})")
        
        # Process the candle data
        result = {
            "original_id": candle_data["id"],
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "processed_at": time.time(),
            "original_close": candle_data["close"],
            "sma_value": candle_data["close"] * 0.98,  # Simulated moving average
            "processing_service": "ServiceB"
        }
        
        # Store the processing result in cache
        result_key = f"signal:analysis:{exchange}:{symbol}:{candle_data['id']}"
        self.cache_service.set(result_key, result, expiry=CacheTTL.HOUR)
        
        # Track this result in a hash
        hash_key = f"analysis:{exchange}:{symbol}:processed"
        self.cache_service.hash_set(
            hash_key,
            candle_data["id"],
            result
        )
        
        # Track this result
        self.processed_data.append(result)
        
        # Check for signal generation (simplified example)
        if candle_data["close"] > candle_data["open"] and counter_is_even(candle_data["timestamp"]):
            # Generate a signal
            signal = {
                "signal_id": str(uuid.uuid4()),
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "candle_id": candle_data["id"],
                "type": "ORDER_BLOCK_DETECTED",
                "direction": "long",
                "timestamp": time.time(),
                "entry_price": candle_data["close"],
                "stop_loss": candle_data["low"] * 0.98,
                "take_profit": candle_data["close"] * 1.05
            }
            
            # Publish the signal with updated routing key format
            signal_routing_key = RoutingKeys.ORDER_BLOCK_DETECTED.format(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe
            )
            
            self.publisher_queue.publish(
                Exchanges.STRATEGY,
                signal_routing_key,
                signal
            )
            
            logger.info(f"ServiceB generated order block signal for {exchange}:{symbol} (ID: {signal['signal_id']})")

# Helper function for even/odd determination based on timestamp
def counter_is_even(timestamp):
    return int(timestamp) % 2 == 0

class ServiceC:
    """Simulates a secondary consumer (e.g., Execution Service)"""
    
    def __init__(self, queue_service, cache_service):
        self.queue_service = queue_service
        self.cache_service = cache_service

        # Setup queue for receiving order block signals
        self.queue_service.declare_queue(Queues.SIGNALS)
        
        # Bind to listen for order block detection events
        self.queue_service.bind_queue(
            Exchanges.STRATEGY,
            Queues.SIGNALS,
            "orderblock.detected.#"  # Subscribe to all order block events
        )
        
        # Subscribe to the queue
        self.queue_service.subscribe(
            Queues.SIGNALS,
            self.on_signal_received
        )
        
        logger.info("ServiceC initialized and subscribed to order block signals")
    
    def on_signal_received(self, signal):
        """Handle incoming order block signal messages"""
        exchange = signal["exchange"]
        symbol = signal["symbol"]
        timeframe = signal["timeframe"]
        
        logger.info(f"ServiceC received order block signal for {exchange}:{symbol} (ID: {signal['signal_id']})")
        
        # Create an order based on the signal
        order = {
            "order_id": str(uuid.uuid4()),
            "signal_id": signal["signal_id"],
            "exchange": exchange,
            "symbol": symbol,
            "type": "LIMIT",
            "side": "BUY" if signal["direction"] == "long" else "SELL",
            "price": signal["entry_price"],
            "quantity": 0.01,  # Simplified quantity calculation
            "stop_loss": signal["stop_loss"],
            "take_profit": signal["take_profit"],
            "status": "PENDING",
            "created_at": time.time()
        }
        
        # Store the order in cache
        order_key = CacheKeys.ORDER.format(
            exchange=exchange,
            symbol=symbol,
            order_id=order["order_id"]
        )
        self.cache_service.set(order_key, order, expiry=CacheTTL.DAY)
        
        # Add to active orders for this symbol
        active_orders_key = CacheKeys.ACTIVE_ORDERS.format(
            exchange=exchange,
            symbol=symbol
        )
        self.cache_service.hash_set(
            active_orders_key,
            order["order_id"],
            {
                "order_id": order["order_id"],
                "price": order["price"],
                "quantity": order["quantity"],
                "created_at": order["created_at"],
                "status": order["status"]
            }
        )
        
        # Publish the order to execution exchange
        order_routing_key = RoutingKeys.ORDER_NEW.format(
            exchange=exchange,
            symbol=symbol
        )
        
        self.queue_service.publish(
            Exchanges.EXECUTION,
            order_routing_key,
            order
        )
        
        logger.info(f"ServiceC created order for {exchange}:{symbol} (ID: {order['order_id']})")

def monitor_services(cache_service):
    """Monitors and displays statistics about the services"""
    while True:
        try:
            # Find all active order keys
            active_order_patterns = cache_service.keys("orders:*:active")
            total_orders = 0
            orders_by_exchange = {}
            
            for pattern in active_order_patterns:
                orders = cache_service.hash_getall(pattern)
                total_orders += len(orders)
                
                # Extract exchange from the pattern (orders:exchange:symbol:active)
                parts = pattern.split(":")
                if len(parts) >= 2:
                    exchange = parts[1]
                    symbol = parts[2]
                    
                    if exchange not in orders_by_exchange:
                        orders_by_exchange[exchange] = {}
                        
                    if symbol not in orders_by_exchange[exchange]:
                        orders_by_exchange[exchange][symbol] = 0
                        
                    orders_by_exchange[exchange][symbol] += len(orders)
            
            # Get latest candles across exchanges
            latest_candle_patterns = cache_service.keys("candle:*:*:*:latest")
            latest_candles = {}
            
            for pattern in latest_candle_patterns[:5]:  # Limit to first 5 for display
                candle_data = cache_service.get(pattern)
                if candle_data:
                    parts = pattern.split(":")
                    if len(parts) >= 4:
                        exchange = parts[1]
                        symbol = parts[2]
                        timeframe = parts[3]
                        
                        if exchange not in latest_candles:
                            latest_candles[exchange] = {}
                        
                        latest_candles[exchange][f"{symbol}_{timeframe}"] = candle_data["close"]
            
            # Print status
            logger.info("=== SYSTEM STATUS ===")
            logger.info(f"Total active orders: {total_orders}")
            
            for exchange, symbols in orders_by_exchange.items():
                logger.info(f"  {exchange}: {sum(symbols.values())} orders across {len(symbols)} symbols")
            
            logger.info("Latest candle prices:")
            for exchange, symbols in latest_candles.items():
                for symbol_tf, price in symbols.items():
                    logger.info(f"  {exchange} {symbol_tf}: {price}")
            
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
        # Initialize services with separate connections
        publisher_queue = QueueService(host='localhost')     # For publishing operations
        consumer_queue_b = QueueService(host='localhost')    # For ServiceB consuming
        consumer_queue_c = QueueService(host='localhost')    # For ServiceC consuming
        cache_service = CacheService(host='localhost')
        
        logger.info("Services initialized successfully")
        
        # Clear any existing test data in cache
        for pattern in ["candle:*", "signal:*", "order:*", "ob:*"]:
            test_keys = cache_service.keys(pattern)
            for key in test_keys:
                cache_service.delete(key)
        
        # Set up exchanges on all connections
        publisher_queue.declare_exchange(Exchanges.MARKET_DATA)
        publisher_queue.declare_exchange(Exchanges.STRATEGY)
        publisher_queue.declare_exchange(Exchanges.EXECUTION)
        
        consumer_queue_b.declare_exchange(Exchanges.MARKET_DATA)
        consumer_queue_b.declare_exchange(Exchanges.STRATEGY)
        
        consumer_queue_c.declare_exchange(Exchanges.STRATEGY)
        consumer_queue_c.declare_exchange(Exchanges.EXECUTION)
        
        # Initialize services with appropriate connections
        service_a = ServiceA(publisher_queue, cache_service)  # Market data producer
        
        # Pass dedicated connections to each service
        service_b = ServiceB(consumer_queue_b, publisher_queue, cache_service)  # Strategy service
        
        # ServiceC gets its own consumer connection (Execution service)
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