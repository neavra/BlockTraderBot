import redis
import json
import logging
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

class CacheService:
    """
    A service for caching and retrieving data across different layers of the trading bot.
    This implementation uses Redis as the cache backend.
    """
    
    def __init__(self, host='localhost', port=6379, db=0, password=None, 
                 socket_timeout=5, decode_responses=True):
        """Initialize the cache service with connection parameters."""
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.decode_responses = decode_responses
        self.redis = None
        
        # Connect to Redis
        self._connect()
    
    def _connect(self):
        """Establish connection to Redis server."""
        try:
            self.redis = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                socket_timeout=self.socket_timeout,
                decode_responses=self.decode_responses
            )
            # Test connection
            self.redis.ping()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
    
    def _ensure_connection(self):
        """Ensure we have an active Redis connection."""
        try:
            if self.redis is None:
                self._connect()
            # Simple ping to check connection
            self.redis.ping()
        except (redis.ConnectionError, redis.TimeoutError):
            logger.warning("Redis connection lost, reconnecting...")
            self._connect()
    
    def get(self, key: str) -> Any:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key to retrieve
            
        Returns:
            Cached value, or None if not found
        """
        try:
            self._ensure_connection()
            value = self.redis.get(key)
            
            if value is None:
                logger.debug(f"Cache miss for key: {key}")
                return None
            
            try:
                # Try to parse as JSON
                return json.loads(value)
            except (TypeError, json.JSONDecodeError):
                # Return as is if not JSON
                return value
                
        except Exception as e:
            logger.error(f"Error retrieving from cache for key {key}: {str(e)}")
            return None
    
    def set(self, key: str, value: Any, expiry: Optional[int] = None) -> bool:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key to set
            value: Value to cache (will be JSON serialized if not a string)
            expiry: Optional TTL in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self._ensure_connection()
            
            # Convert value to JSON if not already a string
            if not isinstance(value, (str, bytes, int, float)):
                value = json.dumps(value)
            
            # Set in Redis
            if expiry is not None:
                result = self.redis.setex(key, expiry, value)
            else:
                result = self.redis.set(key, value)
                
            logger.debug(f"Set cache key: {key}")
            return result
            
        except Exception as e:
            logger.error(f"Error setting cache for key {key}: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.
        
        Args:
            key: Cache key to delete
            
        Returns:
            True if key was deleted, False otherwise
        """
        try:
            self._ensure_connection()
            result = self.redis.delete(key) > 0
            logger.debug(f"Deleted cache key: {key}")
            return result
        except Exception as e:
            logger.error(f"Error deleting cache key {key}: {str(e)}")
            return False
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache.
        
        Args:
            key: Cache key to check
            
        Returns:
            True if key exists, False otherwise
        """
        try:
            self._ensure_connection()
            return self.redis.exists(key) > 0
        except Exception as e:
            logger.error(f"Error checking existence of cache key {key}: {str(e)}")
            return False
    
    def keys(self, pattern: str) -> List[str]:
        """
        Find keys matching a pattern.
        
        Args:
            pattern: Pattern to match (e.g., "user:*")
            
        Returns:
            List of matching keys
        """
        try:
            self._ensure_connection()
            return self.redis.keys(pattern)
        except Exception as e:
            logger.error(f"Error finding keys with pattern {pattern}: {str(e)}")
            return []
    
    def incr(self, key: str, amount: int = 1) -> int:
        """
        Increment a numeric value in the cache.
        
        Args:
            key: Cache key to increment
            amount: Amount to increment by
            
        Returns:
            New value, or None if operation failed
        """
        try:
            self._ensure_connection()
            return self.redis.incrby(key, amount)
        except Exception as e:
            logger.error(f"Error incrementing cache key {key}: {str(e)}")
            return None
    
    def hash_set(self, name: str, key: str, value: Any) -> bool:
        """
        Set a field in a hash.
        
        Args:
            name: Hash name
            key: Field name
            value: Field value
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self._ensure_connection()
            
            # Convert value to JSON if not already a string
            if not isinstance(value, (str, bytes, int, float)):
                value = json.dumps(value)
                
            return self.redis.hset(name, key, value)
        except Exception as e:
            logger.error(f"Error setting hash field {name}.{key}: {str(e)}")
            return False
    
    def hash_get(self, name: str, key: str) -> Any:
        """
        Get a field from a hash.
        
        Args:
            name: Hash name
            key: Field name
            
        Returns:
            Field value, or None if not found
        """
        try:
            self._ensure_connection()
            value = self.redis.hget(name, key)
            
            if value is None:
                return None
                
            try:
                # Try to parse as JSON
                return json.loads(value)
            except (TypeError, json.JSONDecodeError):
                # Return as is if not JSON
                return value
                
        except Exception as e:
            logger.error(f"Error getting hash field {name}.{key}: {str(e)}")
            return None
    
    def hash_getall(self, name: str) -> Dict:
        """
        Get all fields from a hash.
        
        Args:
            name: Hash name
            
        Returns:
            Dictionary of all fields, or empty dict if not found
        """
        try:
            self._ensure_connection()
            result = self.redis.hgetall(name)
            
            # Try to JSON decode values
            if result:
                for key, value in result.items():
                    try:
                        result[key] = json.loads(value)
                    except (TypeError, json.JSONDecodeError):
                        # Keep as is if not JSON
                        pass
                        
            return result
            
        except Exception as e:
            logger.error(f"Error getting all hash fields for {name}: {str(e)}")
            return {}
    
    def publish(self, channel: str, message: Any) -> int:
        """
        Publish a message to a Redis channel.
        Useful for lightweight pub/sub within the application.
        
        Args:
            channel: Channel name
            message: Message to publish
            
        Returns:
            Number of clients that received the message
        """
        try:
            self._ensure_connection()
            
            # Convert message to JSON if not already a string
            if not isinstance(message, str):
                message = json.dumps(message)
                
            return self.redis.publish(channel, message)
            
        except Exception as e:
            logger.error(f"Error publishing to channel {channel}: {str(e)}")
            return 0
    
    def add_to_sorted_set(self, name: str, value: str, score: float, ex: Optional[int] = None) -> bool:
        """
        Add a value to a sorted set with the specified score.
        
        Args:
            name: Sorted set name
            value: Member value
            score: Score for sorting
            ex: Optional TTL in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self._ensure_connection()
            return self.redis.zadd(name, {value: score}) >= 0
        except Exception as e:
            logger.error(f"Error adding to sorted set {name}: {str(e)}")
            return False
    
    def get_from_sorted_set(self, name: str, start: int = 0, end: int = -1, 
                          desc: bool = False) -> List:
        """
        Get values from a sorted set within the specified range.
        
        Args:
            name: Sorted set name
            start: Start index
            end: End index (-1 for all)
            desc: Whether to return in descending order
            
        Returns:
            List of values
        """
        try:
            self._ensure_connection()
            if desc:
                return self.redis.zrevrange(name, start, end)
            else:
                return self.redis.zrange(name, start, end)
        except Exception as e:
            logger.error(f"Error getting from sorted set {name}: {str(e)}")
            return []
    
    def flush(self) -> bool:
        """
        Clear all keys in the current database.
        USE WITH CAUTION!
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self._ensure_connection()
            self.redis.flushdb()
            logger.warning("Redis database flushed!")
            return True
        except Exception as e:
            logger.error(f"Error flushing Redis database: {str(e)}")
            return False
    
    def close(self):
        """Close the Redis connection."""
        if self.redis:
            try:
                self.redis.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {str(e)}")