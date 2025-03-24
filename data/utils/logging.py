import logging
import os
import sys
import json
from datetime import datetime
import traceback
from functools import wraps
from typing import Callable, Any, Dict, Optional

# Configure root logger
def configure_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    log_file: Optional[str] = None
) -> None:
    """
    Configure the logging system.
    
    Args:
        log_level: Logging level (default: INFO)
        log_format: Format to use - 'json' or 'text' (default: json)
        log_file: Optional file path to write logs to
    """
    # Convert string level to logging level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Remove any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set level
    root_logger.setLevel(numeric_level)
    
    # Create formatter
    if log_format.lower() == "json":
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    
    # Create handlers
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    
    # File handler if requested
    if log_file:
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    # Add handlers to root logger
    for handler in handlers:
        root_logger.addHandler(handler)


class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after parsing the log record.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as JSON.
        
        Args:
            record: LogRecord instance
            
        Returns:
            JSON string
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if available
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": "".join(traceback.format_exception(*record.exc_info))
            }
        
        # Add extra attributes
        if hasattr(record, "extra"):
            log_data.update(record.extra)
        
        return json.dumps(log_data)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_execution_time(logger: Optional[logging.Logger] = None):
    """
    Decorator to log function execution time.
    
    Args:
        logger: Optional logger to use, if None will use function's module logger
    
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get logger
            nonlocal logger
            if logger is None:
                logger = logging.getLogger(func.__module__)
            
            # Log start
            start_time = datetime.now()
            logger.debug(
                f"Started execution of {func.__name__}",
                extra={"function": func.__name__}
            )
            
            # Execute function
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                # Log error
                execution_time = (datetime.now() - start_time).total_seconds()
                logger.error(
                    f"Error in {func.__name__}: {str(e)}",
                    extra={
                        "function": func.__name__,
                        "execution_time": execution_time
                    },
                    exc_info=True
                )
                raise
            
            # Log end
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.debug(
                f"Finished execution of {func.__name__} in {execution_time:.4f}s",
                extra={
                    "function": func.__name__,
                    "execution_time": execution_time
                }
            )
            
            return result
        return wrapper
    return decorator