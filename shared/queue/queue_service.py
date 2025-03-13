# shared/queue/service.py
import pika
import json
import logging
import threading
import time
from typing import Dict, Callable, Any, Optional, List

logger = logging.getLogger(__name__)

class QueueService:
    """
    A general-purpose messaging service for the trading bot.
    
    This service provides a flexible interface for publishing and subscribing to
    any exchanges and queues within the application.
    """
    
    def __init__(self, host='localhost', port=5672, username='guest', password='guest'):
        """Initialize the queue service with connection parameters."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None
        self.consumer_thread = None
        self.is_consuming = False
        self.callback_registry = {}
        self.declared_exchanges = set()
        self.declared_queues = set()
        self.queue_bindings = {}  # Maps queue names to a list of (exchange, routing_key) tuples
        
        # Connect to RabbitMQ
        self._connect()
        
    def _connect(self):
        """Establish connection to RabbitMQ server and set up channel."""
        try:
            # Create connection parameters
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            # Connect to RabbitMQ
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            logger.info("Successfully connected to RabbitMQ")
            
            # Re-declare all exchanges and queues after reconnect
            self._redeclare_all()
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise
    
    def _redeclare_all(self):
        """Redeclare all exchanges, queues, and bindings after a reconnection."""
        # Redeclare exchanges
        for exchange in self.declared_exchanges:
            self.channel.exchange_declare(
                exchange=exchange,
                exchange_type='topic',  # Using topic for most flexibility
                durable=True
            )
        
        # Redeclare queues
        for queue in self.declared_queues:
            self.channel.queue_declare(
                queue=queue,
                durable=True
            )
        
        # Redeclare bindings
        for queue, bindings in self.queue_bindings.items():
            for exchange, routing_key in bindings:
                self.channel.queue_bind(
                    exchange=exchange,
                    queue=queue,
                    routing_key=routing_key
                )
    
    def declare_exchange(self, exchange: str, exchange_type: str = 'topic') -> None:
        """
        Declare an exchange if it doesn't exist.
        
        Args:
            exchange: Name of the exchange
            exchange_type: Type of exchange ('direct', 'topic', 'fanout', etc.)
        """
        try:
            if not self.connection or self.connection.is_closed:
                self._connect()
                
            self.channel.exchange_declare(
                exchange=exchange,
                exchange_type=exchange_type,
                durable=True
            )
            
            self.declared_exchanges.add(exchange)
            logger.info(f"Declared exchange: {exchange}")
            
        except Exception as e:
            logger.error(f"Failed to declare exchange {exchange}: {str(e)}")
            raise
    
    def declare_queue(self, queue: str) -> None:
        """
        Declare a queue if it doesn't exist.
        
        Args:
            queue: Name of the queue
        """
        try:
            if not self.connection or self.connection.is_closed:
                self._connect()
                
            self.channel.queue_declare(
                queue=queue,
                durable=True
            )
            
            self.declared_queues.add(queue)
            if queue not in self.queue_bindings:
                self.queue_bindings[queue] = []
                
            logger.info(f"Declared queue: {queue}")
            
        except Exception as e:
            logger.error(f"Failed to declare queue {queue}: {str(e)}")
            raise
    
    def bind_queue(self, exchange: str, queue: str, routing_key: str) -> None:
        """
        Bind a queue to an exchange with a routing key.
        
        Args:
            exchange: Name of the exchange
            queue: Name of the queue
            routing_key: Routing key for the binding
        """
        try:
            if not self.connection or self.connection.is_closed:
                self._connect()
            
            # Ensure exchange and queue exist
            if exchange not in self.declared_exchanges:
                self.declare_exchange(exchange)
            
            if queue not in self.declared_queues:
                self.declare_queue(queue)
            
            # Create the binding
            self.channel.queue_bind(
                exchange=exchange,
                queue=queue,
                routing_key=routing_key
            )
            
            # Store the binding
            if queue not in self.queue_bindings:
                self.queue_bindings[queue] = []
            
            binding = (exchange, routing_key)
            if binding not in self.queue_bindings[queue]:
                self.queue_bindings[queue].append(binding)
            
            logger.info(f"Bound queue {queue} to exchange {exchange} with routing key {routing_key}")
            
        except Exception as e:
            logger.error(f"Failed to bind queue {queue} to exchange {exchange}: {str(e)}")
            raise
    
    def publish(self, exchange: str, routing_key: str, message: Any) -> None:
        """
        Publish a message to an exchange with a routing key.
        
        Args:
            exchange: Name of the exchange
            routing_key: Routing key for message
            message: Message data (will be converted to JSON if not a string)
        """
        try:
            # Ensure we have a connection
            if not self.connection or self.connection.is_closed:
                self._connect()
            
            # Ensure exchange exists
            if exchange not in self.declared_exchanges:
                self.declare_exchange(exchange)
            
            # Convert data to JSON if not already a string
            if not isinstance(message, str):
                message = json.dumps(message)
            
            # Publish the message
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            
            logger.debug(f"Published message to {exchange}:{routing_key}")
            
        except Exception as e:
            logger.error(f"Failed to publish message to {exchange}:{routing_key}: {str(e)}")
            # Try to reconnect for the next message
            self._connect()
            raise
    
    def subscribe(self, queue: str, callback: Callable[[Dict], None]) -> None:
        """
        Subscribe to messages from a queue.
        
        Args:
            queue: Name of the queue
            callback: Function to call when a message is received
        """
        try:
            # Ensure we have a connection
            if not self.connection or self.connection.is_closed:
                self._connect()
            
            # Ensure queue exists
            if queue not in self.declared_queues:
                logger.warning(f"Queue {queue} not declared yet, declaring now")
                self.declare_queue(queue)
            
            # Store the callback
            self.callback_registry[queue] = callback
            
            # Set up consumer for the queue
            self.channel.basic_consume(
                queue=queue,
                on_message_callback=lambda ch, method, props, body: 
                    self._on_message(ch, method, props, body, queue),
                auto_ack=False  # We'll manually acknowledge
            )
            
            logger.info(f"Subscribed to queue: {queue}")
            
            # Start consuming in a separate thread if not already
            if not self.is_consuming:
                self._start_consuming()
                
        except Exception as e:
            logger.error(f"Failed to subscribe to queue {queue}: {str(e)}")
            # Try to reconnect
            self._connect()
            raise
    
    def _on_message(self, channel, method, properties, body, queue):
        """
        Internal callback for handling received messages.
        
        Args:
            channel: The channel object
            method: The method frame
            properties: The properties
            body: The message body
            queue: The queue this message came from
        """
        try:
            # Parse the message body
            message = json.loads(body)
            
            # Get the callback for this queue
            callback = self.callback_registry.get(queue)
            
            if callback:
                # Process the message
                callback(message)
                
                # Acknowledge message only after successful processing
                channel.basic_ack(delivery_tag=method.delivery_tag)
                logger.debug(f"Message acknowledged from queue: {queue}")
            else:
                # No callback found for this queue
                logger.warning(f"No callback registered for queue: {queue}")
                # Still acknowledge the message to remove it from the queue
                channel.basic_ack(delivery_tag=method.delivery_tag)
                
        except Exception as e:
            logger.error(f"Error processing message from queue {queue}: {str(e)}")
            # Negative acknowledge - requeue the message for retry
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def setup_queue(self, exchange: str, queue: str, routing_key: str) -> None:
        """
        Convenience method to setup a complete exchange-queue-binding in one call.
        
        Args:
            exchange: Name of the exchange
            queue: Name of the queue
            routing_key: Routing key for the binding
        """
        self.declare_exchange(exchange)
        self.declare_queue(queue)
        self.bind_queue(exchange, queue, routing_key)
    
    def _start_consuming(self):
        """Start consuming messages in a separate thread."""
        def consume_loop():
            self.is_consuming = True
            logger.info("Starting message consumption loop")
            try:
                self.channel.start_consuming()
            except Exception as e:
                logger.error(f"Consuming interrupted: {str(e)}")
            finally:
                self.is_consuming = False
                logger.info("Message consumption stopped")
        
        # Create and start the consumer thread
        self.consumer_thread = threading.Thread(target=consume_loop)
        self.consumer_thread.daemon = True  # Thread will exit when main thread exits
        self.consumer_thread.start()
    
    def stop(self):
        """Stop consuming messages and close connections."""
        logger.info("Stopping queue service...")
        if self.channel and self.channel.is_open:
            if self.is_consuming:
                self.channel.stop_consuming()
            
            # Wait for consumer thread to finish
            if self.consumer_thread and self.consumer_thread.is_alive():
                self.consumer_thread.join(timeout=5.0)
        
        if self.connection and self.connection.is_open:
            self.connection.close()
            
        logger.info("Queue service stopped")