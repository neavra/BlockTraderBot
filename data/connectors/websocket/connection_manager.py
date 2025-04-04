# import asyncio
# import logging
# from typing import Callable, Optional, Any

# class WebSocketConnectionManager:
#     """
#     Manages WebSocket connections with automatic reconnection.
#     """
    
#     def __init__(self, 
#                  connection_factory: Callable[[], Any],
#                  max_retries: int = 10,
#                  retry_delay: float = 5.0,
#                  logger: Optional[logging.Logger] = None):
#         """
#         Initialize the connection manager.
        
#         Args:
#             connection_factory: Function that creates a new WebSocket connection
#             max_retries: Maximum number of reconnection attempts
#             retry_delay: Delay between reconnection attempts in seconds
#             logger: Logger instance
#         """
#         self.connection_factory = connection_factory
#         self.max_retries = max_retries
#         self.retry_delay = retry_delay
#         self.connection = None
#         self.logger = logger or logging.getLogger("WebSocketConnectionManager")
#         self.is_running = False
#         self.retry_count = 0
    
#     async def connect(self):
#         """
#         Establish a WebSocket connection with retry logic.
#         """
#         self.is_running = True
#         self.retry_count = 0
        
#         while self.is_running and self.retry_count < self.max_retries:
#             try:
#                 self.connection = await self.connection_factory()
#                 self.retry_count = 0
#                 self.logger.info("WebSocket connection established successfully")
#                 return self.connection
            
#             except Exception as e:
#                 self.retry_count += 1
#                 self.logger.error(f"WebSocket connection failed (attempt {self.retry_count}/{self.max_retries}): {str(e)}")
                
#                 if self.retry_count >= self.max_retries:
#                     self.logger.critical("Maximum retry attempts reached. Giving up.")
#                     self.is_running = False
#                     raise
                
#                 await asyncio.sleep(self.retry_delay)
        
#         return None
    
#     async def disconnect(self):
#         """
#         Close the WebSocket connection.
#         """
#         self.is_running = False
#         if self.connection and hasattr(self.connection, 'close'):
#             await self.connection.close()
#             self.logger.info("WebSocket connection closed")
#         self.connection = None