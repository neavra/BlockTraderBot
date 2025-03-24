import asyncio
import threading
import concurrent.futures
from typing import List, Callable, Any, TypeVar, Generic, Coroutine, Dict, Optional, Union
from functools import wraps
import time
import logging

# Generic type for function inputs
T = TypeVar('T')
# Generic type for function outputs
R = TypeVar('R')

logger = logging.getLogger(__name__)


def run_in_thread(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to run a function in a separate thread.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        thread.daemon = True
        thread.start()
        return thread
    return wrapper


def run_in_threadpool(
    func: Callable[..., R],
    items: List[T],
    max_workers: int = None,
    **kwargs
) -> List[R]:
    """
    Run a function over a list of items using a thread pool.
    
    Args:
        func: Function to execute for each item
        items: List of items to process
        max_workers: Maximum number of worker threads
        **kwargs: Additional keyword arguments to pass to func
        
    Returns:
        List of results
    """
    if not items:
        return []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(func, item, **kwargs) for item in items]
        return [future.result() for future in concurrent.futures.as_completed(futures)]


def run_in_processpool(
    func: Callable[..., R],
    items: List[T],
    max_workers: int = None,
    **kwargs
) -> List[R]:
    """
    Run a function over a list of items using a process pool.
    
    Args:
        func: Function to execute for each item
        items: List of items to process
        max_workers: Maximum number of worker processes
        **kwargs: Additional keyword arguments to pass to func
        
    Returns:
        List of results
    """
    if not items:
        return []
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(func, item, **kwargs) for item in items]
        return [future.result() for future in concurrent.futures.as_completed(futures)]


async def gather_with_concurrency(n: int, *tasks) -> List[Any]:
    """
    Run coroutines with a limit on concurrency.
    
    Args:
        n: Maximum number of concurrent tasks
        *tasks: Coroutines to run
        
    Returns:
        List of results
    """
    semaphore = asyncio.Semaphore(n)
    
    async def sem_task(task):
        async with semaphore:
            return await task
    
    return await asyncio.gather(*(sem_task(task) for task in tasks))


class RateLimiter:
    """
    Rate limiter for controlling API request frequency.
    """
    
    def __init__(self, max_calls: int, period: float = 1.0):
        """
        Initialize the rate limiter.
        
        Args:
            max_calls: Maximum number of calls allowed in the period
            period: Time period in seconds
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self._lock = threading.RLock()
    
    def __call__(self, func: Callable) -> Callable:
        """
        Decorator for rate-limiting a function.
        
        Args:
            func: Function to decorate
            
        Returns:
            Rate-limited function
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self._lock:
                # Remove old calls
                current_time = time.time()
                self.calls = [call_time for call_time in self.calls 
                             if current_time - call_time <= self.period]
                
                # Check if we're at the limit
                if len(self.calls) >= self.max_calls:
                    # We need to wait
                    oldest_call = self.calls[0]
                    sleep_time = self.period - (current_time - oldest_call)
                    if sleep_time > 0:
                        logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                        time.sleep(sleep_time)
                
                # Add the current call and execute
                self.calls.append(time.time())
            
            return func(*args, **kwargs)
        return wrapper
    
    async def __call_async__(self, func: Callable) -> Callable:
        """
        Decorator for rate-limiting an async function.
        
        Args:
            func: Async function to decorate
            
        Returns:
            Rate-limited async function
        """
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Use a blocking approach with a lock for simplicity
            # In a more sophisticated implementation, we'd use asyncio primitives
            with self._lock:
                # Remove old calls
                current_time = time.time()
                self.calls = [call_time for call_time in self.calls 
                             if current_time - call_time <= self.period]
                
                # Check if we're at the limit
                if len(self.calls) >= self.max_calls:
                    # We need to wait
                    oldest_call = self.calls[0]
                    sleep_time = self.period - (current_time - oldest_call)
                    if sleep_time > 0:
                        logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                        await asyncio.sleep(sleep_time)
                
                # Add the current call and execute
                self.calls.append(time.time())
            
            return await func(*args, **kwargs)
        return wrapper


class AsyncBatchProcessor(Generic[T, R]):
    """
    Process items in batches with controlled concurrency.
    """
    
    def __init__(
        self,
        processor: Callable[[List[T]], Coroutine[Any, Any, List[R]]],
        batch_size: int = 100,
        max_concurrency: int = 5
    ):
        """
        Initialize the batch processor.
        
        Args:
            processor: Async function that processes a batch of items
            batch_size: Maximum items per batch
            max_concurrency: Maximum number of concurrent batches
        """
        self.processor = processor
        self.batch_size = batch_size
        self.max_concurrency = max_concurrency
    
    async def process(self, items: List[T]) -> List[R]:
        """
        Process a list of items in batches.
        
        Args:
            items: Items to process
            
        Returns:
            Combined results from all batches
        """
        if not items:
            return []
        
        # Split into batches
        batches = [
            items[i:i + self.batch_size]
            for i in range(0, len(items), self.batch_size)
        ]
        
        # Process batches with limited concurrency
        tasks = [self.processor(batch) for batch in batches]
        batch_results = await gather_with_concurrency(self.max_concurrency, *tasks)
        
        # Combine results
        results = []
        for batch_result in batch_results:
            results.extend(batch_result)
        
        return results