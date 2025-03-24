import logging
import traceback
import functools
import time
from typing import Callable, Type, Tuple, Optional, Any, List, Dict, TypeVar
import asyncio

logger = logging.getLogger(__name__)

# Type variable for function return
R = TypeVar('R')


class TradingBotError(Exception):
    """Base exception for all trading bot errors."""
    pass


class ConnectorError(TradingBotError):
    """Error related to exchange connectors."""
    pass


class APIError(ConnectorError):
    """Error returned from an external API."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class RateLimitError(APIError):
    """Rate limit exceeded on an API call."""
    pass


class AuthenticationError(APIError):
    """Authentication failed with an API."""
    pass


class DatabaseError(TradingBotError):
    """Error related to database operations."""
    pass


class ValidationError(TradingBotError):
    """Error related to data validation."""
    pass


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None
) -> Callable:
    """
    Retry decorator for functions that might fail temporarily.
    
    Args:
        max_attempts: Maximum number of attempts
        delay_seconds: Initial delay between retries in seconds
        backoff_factor: Factor by which the delay increases with each retry
        exceptions: Types of exceptions to catch and retry
        on_retry: Optional callback to execute before each retry
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 1
            last_exception = None
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    # Check if we should retry
                    if attempt < max_attempts:
                        # Calculate delay
                        delay = delay_seconds * (backoff_factor ** (attempt - 1))
                        
                        # Log the retry
                        logger.warning(
                            f"Retry {attempt}/{max_attempts} for {func.__name__} "
                            f"after {delay:.2f}s due to: {str(e)}"
                        )
                        
                        # Call on_retry callback if provided
                        if on_retry:
                            on_retry(e, attempt)
                        
                        # Sleep before retrying
                        time.sleep(delay)
                        
                    attempt += 1
            
            # If we've exhausted all retries, raise the last exception
            logger.error(
                f"All {max_attempts} retry attempts failed for {func.__name__}"
            )
            raise last_exception
        
        return wrapper
    
    return decorator


def retry_async(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None
) -> Callable:
    """
    Retry decorator for async functions that might fail temporarily.
    
    Args:
        max_attempts: Maximum number of attempts
        delay_seconds: Initial delay between retries in seconds
        backoff_factor: Factor by which the delay increases with each retry
        exceptions: Types of exceptions to catch and retry
        on_retry: Optional callback to execute before each retry
        
    Returns:
        Decorated async function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            attempt = 1
            last_exception = None
            
            while attempt <= max_attempts:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    # Check if we should retry
                    if attempt < max_attempts:
                        # Calculate delay
                        delay = delay_seconds * (backoff_factor ** (attempt - 1))
                        
                        # Log the retry
                        logger.warning(
                            f"Retry {attempt}/{max_attempts} for {func.__name__} "
                            f"after {delay:.2f}s due to: {str(e)}"
                        )
                        
                        # Call on_retry callback if provided
                        if on_retry:
                            on_retry(e, attempt)
                        
                        # Sleep before retrying
                        await asyncio.sleep(delay)
                        
                    attempt += 1
            
            # If we've exhausted all retries, raise the last exception
            logger.error(
                f"All {max_attempts} retry attempts failed for {func.__name__}"
            )
            raise last_exception
        
        return wrapper
    
    return decorator


def error_boundary(
    fallback_value: Optional[R] = None,
    log_level: int = logging.ERROR,
    log_stacktrace: bool = True,
    reraise: bool = False
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    Error boundary decorator to handle exceptions gracefully.
    
    Args:
        fallback_value: Value to return if an exception occurs
        log_level: Logging level for the error
        log_stacktrace: Whether to log the full stack trace
        reraise: Whether to reraise the exception after logging
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> R:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Prepare log message
                log_message = f"Error in {func.__name__}: {str(e)}"
                
                # Log the error
                if log_stacktrace:
                    logger.log(log_level, log_message, exc_info=True)
                else:
                    logger.log(log_level, log_message)
                
                # Reraise or return fallback
                if reraise:
                    raise
                return fallback_value
        
        return wrapper
    
    return decorator


def error_boundary_async(
    fallback_value: Optional[R] = None,
    log_level: int = logging.ERROR,
    log_stacktrace: bool = True,
    reraise: bool = False
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    Error boundary decorator for async functions.
    
    Args:
        fallback_value: Value to return if an exception occurs
        log_level: Logging level for the error
        log_stacktrace: Whether to log the full stack trace
        reraise: Whether to reraise the exception after logging
        
    Returns:
        Decorated async function
    """
    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> R:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Prepare log message
                log_message = f"Error in {func.__name__}: {str(e)}"
                
                # Log the error
                if log_stacktrace:
                    logger.log(log_level, log_message, exc_info=True)
                else:
                    logger.log(log_level, log_message)
                
                # Reraise or return fallback
                if reraise:
                    raise
                return fallback_value
        
        return wrapper
    
    return decorator


def exception_to_str(exc: Exception) -> str:
    """
    Convert an exception to a detailed string.
    
    Args:
        exc: The exception to convert
        
    Returns:
        String representation with traceback
    """
    return f"{type(exc).__name__}: {str(exc)}\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}"