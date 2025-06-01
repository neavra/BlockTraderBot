import logging
import asyncio
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService

from execution.exchange.exchange_interface import ExchangeInterface
from shared.constants import Exchanges, Queues, RoutingKeys, CacheKeys, CacheTTL
from shared.domain.dto.order_dto import OrderDto


logger = logging.getLogger(__name__)

class ExecutionService:
    """
    Exchange Executor handles the process of converting trading signals into actual orders.
    
    This component:
    1. Receives signals from the consumer queue
    2. Processes signals to build order objects
    3. Executes orders via the exchange interface
    4. Publishes order events to another queue for tracking
    5. Caches order objects for later reference
    """

    def __init__(
        self, 
        exchange: ExchangeInterface,
        consumer_queue: QueueService, 
        producer_queue: QueueService,
        cache_service: CacheService,
        config: Dict[str, Any]):
        """
        Initialize the Exchange Executor.
        
        Args:
            exchange: Exchange implementation to use for order execution
            consumer_queue: Queue service for consuming signals
            producer_queue: Queue service for producing order events
            cache_service: Cache service for storing order information
            config: Configuration parameters including:
                - queue_settings: Settings for message queues
                - cache_settings: Settings for order caching
                - risk_settings: Risk management parameters
        """
        self.exchange = exchange
        self.consumer_queue = consumer_queue
        self.producer_queue = producer_queue
        self.cache_service = cache_service
        self.config = config
        self.running = False
        
        # Store active signals and orders
        self.active_signals = {}
        self.active_orders = {}
        self.main_loop = None
        
    async def start(self):
        """Initialize and start the execution service components."""
        if self.running:
            logger.warning("Execution service is already running")
            return
        
        logger.info("Starting execution service...")
        
        # Store the event loop for callbacks
        self.main_loop = asyncio.get_running_loop()
        
        # Initialize the exchange connector
        await self._init_exchange()

        # Initialize signal consumer
        await self._init_signal_consumer()

        # Initialize order producer
        await self._init_order_producer()

        self.running = True
        logger.info("Execution service started successfully")
        
    async def stop(self):
        """Stop the execution service and release resources."""
        logger.info("Stopping execution service...")

        if self.exchange:
            await self.exchange.close()

        if self.consumer_queue:
            self.consumer_queue.stop()
        
        if self.producer_queue:
            self.producer_queue.stop()
        
        self.running = False

    async def _init_exchange(self):
        """Initialize the exchange for order execution."""
        logger.info("Initializing exchange...")
        await self.exchange.initialize()
        logger.info("Exchange connector initialized")

    async def _init_signal_consumer(self):
        """Initialize the signal event consumer to process signals generated."""
        logger.info("Initializing signal event consumer...")
        self.consumer_queue.declare_exchange(Exchanges.STRATEGY)
        self.consumer_queue.declare_queue(Queues.SIGNALS)
        self.consumer_queue.bind_queue(
            Exchanges.STRATEGY,
            Queues.SIGNALS,
            RoutingKeys.SIGNAL_ALL
        )

        # Subscribe to signal queue to process trading signals
        self.consumer_queue.subscribe(
            Queues.SIGNALS,
            self._on_signal_received
        )
        
        logger.info("Signal event consumer initialized")

    async def _init_order_producer(self):
        """Initialize the order event producer to produce events when orders are placed."""
        logger.info("Initializing order event producer...")

        # Ensure queue exchange and queue exist
        self.producer_queue.declare_exchange(Exchanges.EXECUTION)
        self.producer_queue.declare_queue(Queues.ORDERS)
        
        # Bind for new orders
        self.producer_queue.bind_queue(
            Exchanges.EXECUTION,
            Queues.ORDERS,
            RoutingKeys.ORDER_NEW
        )
        
        # Bind for cancelled orders
        self.producer_queue.bind_queue(
            Exchanges.EXECUTION,
            Queues.ORDERS,
            RoutingKeys.ORDER_CANCELLED
        )
        
        # Bind for failed orders
        self.producer_queue.bind_queue(
            Exchanges.EXECUTION,
            Queues.ORDERS,
            RoutingKeys.ORDER_FAILED
        )
        
        logger.info("Order event producer initialized")

    def _on_signal_received(self, signal_message: Dict[str, Any]):
        """
        Callback for processing signal messages from the queue.
        This is a synchronous method that will schedule async processing.
        
        Args:
            signal_message: Signal message data from the queue
        """
        try:
            signal_id = signal_message.get('id')
            logger.info(f"Received signal: {signal_id}")
            
            # Schedule asynchronous processing
            asyncio.run_coroutine_threadsafe(
                self._process_signal_async(signal_message),
                self.main_loop
            )
        except Exception as e:
            logger.error(f"Error scheduling signal processing: {e}")

    async def _process_signal_async(self, signal_message: Dict[str, Any]):
        """
        Asynchronous processing of a signal message.
        
        Args:
            signal_message: Signal message data from the queue
        """
        try:
            # Process signal to get order parameters
            order_params = await self.process_signal(signal_message)
            
            if order_params:
                # Execute the order
                order_result = await self.execute_order(order_params)
                
                if order_result:
                    logger.info(f"Order executed successfully: {order_result['id']}")

                    # Create Order object for tracking
                    order = OrderDto(
                        id=order_result['id'],
                        symbol=order_params['symbol'],
                        side=order_params['side'],
                        type=order_params['type'],
                        price=order_params['price'],
                        size=order_params['amount'],
                        status=order_result.get('status', 'open'),
                        timestamp=datetime.now().isoformat()
                    )
                    # Cache the order
                    await self._cache_order(order_result['id'], order)
                    
                    # Publish order event
                    await self._publish_order_event(order, 'created')
                    
                    # Store in active orders
                    self.active_orders[order_result['id']] = order
                else:
                    logger.error(f"Failed to execute order for signal: {signal_message['id']}")
            else:
                logger.warning(f"Signal rejected: {signal_message['id']}")
                
        except Exception as e:
            logger.error(f"Error processing signal: {e}", exc_info=True)

    async def process_signal(self, signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a trading signal and prepare order parameters.
        
        EXECUTION LAYER RESPONSIBILITIES:
        1. Validate signal structure and required fields
        2. Check account liquidity and existing orders
        3. Apply execution-level risk limits (max position size caps)
        4. Handle order conflicts and replacements
        5. Build final order parameters for exchange
        """
        # 1. VALIDATE SIGNAL STRUCTURE
        required_fields = ['id', 'symbol', 'direction', 'signal_type', 'price_target', 'stop_loss', 'take_profit']
        if not all(k in signal for k in required_fields):
            logger.error(f"Invalid signal format - missing required fields: {signal}")
            return None
        
        # Extract signal parameters (all should be provided by strategy)
        signal_id = signal['id']
        symbol = signal['symbol']
        direction = signal['direction']
        signal_type = signal['signal_type']
        price_target = signal['price_target']
        stop_loss = signal['stop_loss']
        take_profit = signal['take_profit']
        
        position_size = signal['position_size']
        if not position_size:
            logger.error(f"Signal {signal_id} missing position_size - strategy should calculate this")
            return None
        
        # 3. CHECK ACCOUNT LIQUIDITY
        # TODO to implement
        available_balance = await self._get_available_balance(symbol)
        required_margin = self._calculate_required_margin(symbol, position_size, price_target)
        
        if available_balance < required_margin:
            logger.warning(f"Insufficient balance for signal {signal_id}. Required: {required_margin}, Available: {available_balance}")
            return None
        
        # 4. APPLY EXECUTION-LEVEL RISK LIMITS
        max_position_size = self.config.get('execution_limits', {}).get('max_position_size', 1.0)
        max_order_value = self.config.get('execution_limits', {}).get('max_order_value', 100000)
        
        if position_size > max_position_size:
            logger.warning(f"Position size {position_size} exceeds execution limit {max_position_size}")
            position_size = max_position_size
        
        order_value = position_size * price_target
        if order_value > max_order_value:
            logger.warning(f"Order value {order_value} exceeds execution limit {max_order_value}")
            return None
        
        # 5. RECORD SIGNAL IN ACTIVE SIGNALS
        self.active_signals[signal_id] = {
            **signal,
            'processed_at': datetime.now().isoformat(),
            'execution_checks_passed': True
        }
        
        # Store signal in cache
        signal_cache_key = f"signal:{self.exchange.id}:{symbol}:{signal_id}"
        self.cache_service.set(
            signal_cache_key,
            self.active_signals[signal_id],
            expiry=CacheTTL.DAY
        )
        
        # 6. BUILD FINAL ORDER PARAMETERS
        order_params = {
            'symbol': symbol,
            'type': 'limit',
            'side': 'buy' if direction == 'long' else 'sell',
            'amount': position_size,
            'price': price_target,
            'params': {
                'signal_id': signal_id,
                'timeInForce': 'GTC',
                'stopLoss': stop_loss,
                'takeProfit': take_profit,
                'leverage': self.config.get('execution_settings', {}).get('default_leverage', 1),
                'reduceOnly': signal_type == 'exit'
            }
        }
        
        logger.info(f"Signal {signal_id} passed execution validation - ready for order placement")
        return order_params

    async def _get_existing_orders(self, symbol: str) -> List[Dict[str, Any]]:
        """Get all existing orders for a symbol"""
        try:
            orders = await self.exchange.fetch_open_orders(symbol)
            return orders
        except Exception as e:
            logger.error(f"Error fetching existing orders: {e}")
            return []

    async def _get_available_balance(self, symbol: str) -> float:
        """Get available balance for trading"""
        try:
            balance = await self.exchange.fetch_balance()
            # Logic to determine available balance based on symbol and margin requirements
            # This is exchange-specific implementation
            return balance.get('free', {}).get('USDT', 0.0)  # Simplified
        except Exception as e:
            logger.error(f"Error fetching balance: {e}")
            return 0.0

    def _calculate_required_margin(self, symbol: str, position_size: float, price: float) -> float:
        """Calculate required margin for the order"""
        leverage = self.config.get('execution_settings', {}).get('default_leverage', 1)
        return (position_size * price) / leverage
        
    async def execute_order(self, order_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Execute a trade order based on processed signal parameters.
        
        This method:
        1. Places the order on the exchange
        """
        try:
            symbol = order_params['symbol']
            order_type = order_params['type']
            side = order_params['side']
            amount = order_params['amount']
            price = order_params['price']
            params = order_params.get('params', {})
            
            logger.info(f"Executing {order_type} {side} order for {symbol}: {amount} @ {price}")
            
            # Place the main order
            order_result = await self.exchange.create_order(
                symbol=symbol,
                order_type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params
            )
            
            if not order_result:
                logger.error("Order execution failed: No result returned from exchange")
                return None
            
            # Handle stop loss and take profit orders if needed
            # This would be implemented based on the exchange's capabilities
            
            return order_result
            
        except Exception as e:
            logger.error(f"Error executing order: {e}", exc_info=True)
            
            # Publish failed order event
            failed_order = OrderDto(
                id=f"failed_{int(datetime.now().timestamp())}",
                symbol=order_params['symbol'],
                side=order_params['side'],
                type=order_params['type'],
                price=order_params['price'],
                size=order_params['amount'],
                status='failed',
                timestamp=datetime.now().isoformat()
            )
            
            await self._publish_order_event(failed_order, 'failed')
            return None
    
    async def cancel_order(self, order_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Cancel an existing order and publish a cancellation event.
        
        Args:
            order_id: ID of the order to cancel
            symbol: Trading pair symbol for the order
            
        Returns:
            Cancellation result or None if the cancellation failed
        """
        try:
            logger.info(f"Cancelling order {order_id} for {symbol}")
            
            # Cancel the order on the exchange
            cancel_result = await self.exchange.cancel_order(
                id=order_id,
                symbol=symbol
            )
            
            if not cancel_result:
                logger.error(f"Order cancellation failed: No result returned from exchange")
                return None
            
            # Get the order from active orders or fetch from exchange
            order = self.active_orders.get(order_id)
            
            if not order:
                # Try to fetch the order details
                try:
                    order_data = await self.exchange.fetch_order(order_id, symbol)
                    order = OrderDto(
                        id=order_data['id'],
                        symbol=symbol,
                        side=order_data.get('side', 'unknown'),
                        type=order_data.get('type', 'unknown'),
                        price=order_data.get('price', 0.0),
                        size=order_data.get('amount', 0.0),
                        status='cancelled',
                        timestamp=datetime.now().isoformat()
                    )
                except Exception as fetch_error:
                    logger.warning(f"Could not fetch order details after cancellation: {fetch_error}")
                    # Create a minimal order object for the event
                    order = OrderDto(
                        id=order_id,
                        symbol=symbol,
                        side='unknown',
                        type='unknown',
                        price=0.0,
                        size=0.0,
                        status='cancelled',
                        timestamp=datetime.now().isoformat()
                    )
            else:
                # Update the status of the cached order
                order.status = 'cancelled'
            
            # Update the cache with cancelled status
            await self._cache_order(order_id, order)
            
            # Remove from active orders
            if order_id in self.active_orders:
                del self.active_orders[order_id]
            
            # Publish cancellation event
            await self._publish_order_event(order, 'cancelled')
            
            logger.info(f"Order {order_id} cancelled successfully")
            return cancel_result
            
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}", exc_info=True)
            return None
    
    async def _cache_order(self, order_id: str, order: OrderDto) -> None:
        """
        Cache an order object for later reference.
        
        This method:
        1. Serializes the order object
        2. Stores it in the cache with appropriate expiration
        """
        try:
            # Create cache key using the format from constants
            order_key = CacheKeys.ORDER.format(
                exchange=self.exchange.id,
                symbol=order.symbol,
                order_id=order_id
            )
            
            # Convert order to dict for storage
            order_dict = {
                'id': order.id,
                'symbol': order.symbol,
                'side': order.side,
                'type': order.type,
                'price': order.price,
                'size': order.size,
                'status': order.status,
                'timestamp': order.timestamp
            }
            
            # Store in cache with expiry
            self.cache_service.set(
                order_key,
                order_dict,
                expiry=CacheTTL.DAY
            )
            
            # Also add to active orders set if order is open
            if order.status == 'open':
                active_orders_key = CacheKeys.ACTIVE_ORDERS.format(
                    exchange=self.exchange.id,
                    symbol=order.symbol
                )
                
                self.cache_service.hash_set(
                    active_orders_key,
                    order_id,
                    order_dict
                )
            
            logger.debug(f"Cached order {order_id} in {order_key}")
            
        except Exception as e:
            logger.error(f"Error caching order {order_id}: {e}")
    
    async def _publish_order_event(self, order: OrderDto, event_type: str) -> None:
        """
        Publish an order event to the tracking queue.
        
        This method:
        1. Creates an event message with order details and event type
        2. Publishes it to the event queue for processing by monitoring services
        """
        try:
            # Create event message
            event = {
                'type': event_type,
                'order_id': order.id,
                'symbol': order.symbol,
                'side': order.side,
                'order_type': order.type,
                'price': order.price,
                'size': order.size,
                'status': order.status,
                'timestamp': order.timestamp
            }
            
            # Determine routing key based on event type
            if event_type == 'created':
                routing_key = RoutingKeys.ORDER_NEW
            elif event_type == 'cancelled':
                routing_key = RoutingKeys.ORDER_CANCELLED
            elif event_type == 'failed':
                routing_key = RoutingKeys.ORDER_FAILED
            else:
                routing_key = f"order.{event_type}.{self.exchange.id}.{order.symbol}"
            
            # Publish to the execution exchange
            self.producer_queue.publish(
                Exchanges.EXECUTION,
                routing_key,
                event
            )
            
            logger.info(f"Published {event_type} event for order {order.id}")
            
        except Exception as e:
            logger.error(f"Error publishing order event: {e}")