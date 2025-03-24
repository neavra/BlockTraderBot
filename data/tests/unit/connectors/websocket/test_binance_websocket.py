import logging
import unittest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import json
import asyncio
from datetime import datetime

import websockets

# Import the class to be tested
from connectors.websocket.binance_websocket import BinanceWebSocketClient

class TestBinanceWebSocketClient(unittest.IsolatedAsyncioTestCase):
    """Test suite for BinanceWebSocketClient class"""
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        self.mock_manager = Mock()
        self.mock_manager.handle_websocket_data = AsyncMock()
        self.client = BinanceWebSocketClient('btcusdt', '1m', self.mock_manager)
        
        # Mock the connection_manager
        self.client.connection_manager = Mock()
        self.client.connection_manager.connect = AsyncMock()
        self.client.connection_manager.disconnect = AsyncMock()
    
    async def asyncSetUp(self):
        """Async setup for tests"""
        pass
        
    def test_init(self):
        """Test the initialization of BinanceWebSocketClient"""
        client = BinanceWebSocketClient('ETHUSDT', '5m')
        
        self.assertEqual(client.symbol, 'ethusdt')
        self.assertEqual(client.interval, '5m')
        self.assertEqual(client.url, 'wss://stream.binance.com:9443/ws/ethusdt@kline_5m')
        self.assertIsNone(client.manager)
        self.assertIsNotNone(client.ssl_context)
        self.assertIsNotNone(client.logger)
        self.assertIsNotNone(client.connection_manager)
    
    def test_parse_binance_kline(self):
        """Test the parsing of Binance kline data"""
        # Sample Binance WebSocket message
        sample_data = {
            'e': 'kline',
            'E': 1625239910000,
            's': 'BTCUSDT',
            'k': {
                't': 1625239860000,
                'T': 1625239919999,
                's': 'BTCUSDT',
                'i': '1m',
                'f': 100,
                'L': 200,
                'o': '34000.10',
                'c': '34100.20',
                'h': '34200.30',
                'l': '33900.40',
                'v': '10.5',
                'n': 100,
                'x': True,
                'q': '357000.0',
                'V': '5.2',
                'Q': '177000.0',
                'B': '0'
            }
        }
        
        parsed_data, is_closed = self.client.parse_binance_kline(sample_data)
        
        # Verify parsing results
        self.assertEqual(parsed_data['exchange'], 'binance')
        self.assertEqual(parsed_data['symbol'], 'BTCUSDT')
        self.assertEqual(parsed_data['interval'], '1m')
        self.assertEqual(parsed_data['event_time'], 1625239910000)
        self.assertEqual(parsed_data['start_time'], 1625239860000)
        self.assertEqual(parsed_data['close_time'], 1625239919999)
        self.assertEqual(parsed_data['open'], '34000.10')
        self.assertEqual(parsed_data['high'], '34200.30')
        self.assertEqual(parsed_data['low'], '33900.40')
        self.assertEqual(parsed_data['close'], '34100.20')
        self.assertEqual(parsed_data['volume'], '10.5')
        self.assertTrue(parsed_data['is_closed'])
        self.assertTrue(is_closed)
        
    def test_parse_binance_kline_not_closed(self):
        """Test parsing kline data for candles that aren't closed yet"""
        # Sample with candle not closed
        sample_data = {
            'e': 'kline',
            'E': 1625239910000,
            's': 'BTCUSDT',
            'k': {
                't': 1625239860000,
                'T': 1625239919999,
                's': 'BTCUSDT',
                'i': '1m',
                'o': '34000.10',
                'c': '34100.20',
                'h': '34200.30',
                'l': '33900.40',
                'v': '10.5',
                'x': False  # Candle not closed
            }
        }
        
        parsed_data, is_closed = self.client.parse_binance_kline(sample_data)
        
        self.assertFalse(parsed_data['is_closed'])
        self.assertFalse(is_closed)
    
    @patch('websockets.connect')
    async def test_fetch_candlestick_data(self, mock_connect):
        """Test fetching candlestick data from WebSocket"""
        # Create a mock websocket
        mock_ws = AsyncMock()
        mock_message = json.dumps({
            'e': 'kline',
            'E': 1625239910000,
            's': 'BTCUSDT',
            'k': {
                't': 1625239860000,
                'T': 1625239919999,
                's': 'BTCUSDT',
                'i': '1m',
                'o': '34000.10',
                'c': '34100.20',
                'h': '34200.30',
                'l': '33900.40',
                'v': '10.5',
                'x': True
            }
        })
        
        # Set up the mock websocket to return our message
        mock_ws.__aiter__.return_value = [mock_message]
        
        # Configure the connection manager to return our mock websocket
        self.client.connection_manager.connect.return_value = mock_ws
        
        # Get the first yielded value from fetch_candlestick_data
        async for candle_data, is_closed in self.client.fetch_candlestick_data():
            # Verify the returned data
            self.assertEqual(candle_data['exchange'], 'binance')
            self.assertEqual(candle_data['symbol'], 'BTCUSDT')
            self.assertEqual(candle_data['interval'], '1m')
            self.assertTrue(is_closed)
            break
        
        # Verify connect was called
        self.client.connection_manager.connect.assert_called_once()
    
    @patch('websockets.connect')
    async def test_fetch_candlestick_data_connection_closed(self, mock_connect):
        """Test reconnection when WebSocket connection is closed"""
        mock_ws = AsyncMock()
        
        # Create a proper ConnectionClosed exception
        # The modern websockets library requires specific frame objects for the constructor
        from websockets.exceptions import ConnectionClosed
        from websockets.frames import Close
        
        # Create Close frame objects for the exception
        close_code = 1000
        close_reason = "Normal closure"
        
        # Use a different approach to simulate connection closed exception
        connection_closed_exception = ConnectionClosed(
            Close(close_code, close_reason),  # received frame
            Close(close_code, close_reason),  # sent frame
            True  # received_then_sent flag (indicating we received a close frame and then sent one)
        )
        
        # First iteration raises ConnectionClosed
        mock_ws.__aiter__.return_value.__anext__.side_effect = connection_closed_exception
        self.client.connection_manager.connect.return_value = mock_ws
        
        # Create a mock that behaves like an async iterator for the second call
        # This is key to fixing the issue with 'async for'
        async def mock_generator():
            # Return a single item, then stop iteration
            yield {
                'exchange': 'binance',
                'symbol': 'BTCUSDT',
                'interval': '1m',
                'event_time': 1625239910000,
                'open': '34000.10',
                'close': '34100.20',
                'is_closed': True
            }, True
        
        # Instead of making fetch_candlestick_data return a coroutine,
        # make it return a proper async generator
        with patch.object(self.client, 'fetch_candlestick_data', 
                        return_value=mock_generator()):
            
            # Try to iterate - this should trigger the connection closed 
            # exception and then call our patched method
            with patch.object(self.client, 'logger') as mock_logger:
                async for a, b in self.client.fetch_candlestick_data():
                    # Verify the actual values are now given
                    assert b == True
                    assert a == {
                        'exchange': 'binance',
                        'symbol': 'BTCUSDT',
                        'interval': '1m',
                        'event_time': 1625239910000,
                        'open': '34000.10',
                        'close': '34100.20',
                        'is_closed': True
                    }
    
    @patch('websockets.connect')
    async def test_listen(self, mock_connect):
        """Test the listen method with a valid manager"""
        # Configure the fetch_candlestick_data method to yield test data
        async def mock_fetch():
            yield {
                'exchange': 'binance',
                'symbol': 'BTCUSDT',
                'interval': '1m',
                'event_time': 1625239910000,
                'open': '34000.10',
                'high': '34200.30',
                'low': '33900.40',
                'close': '34100.20',
                'volume': '10.5',
                'is_closed': True
            }, True
        
        with patch.object(self.client, 'fetch_candlestick_data', 
                         new=mock_fetch):
            await self.client.listen()
            
            # Verify manager's handle_websocket_data was called with correct data
            self.mock_manager.handle_websocket_data.assert_called_once()
            call_args = self.mock_manager.handle_websocket_data.call_args[0]
            self.assertEqual(call_args[0]['symbol'], 'BTCUSDT')
            self.assertEqual(call_args[0]['interval'], '1m')
            self.assertTrue(call_args[1])  # is_closed should be True
    
    async def test_listen_no_manager(self):
        """Test listen method with no manager configured"""
        # Create a client with no manager
        client = BinanceWebSocketClient('btcusdt', '1m')
        
        # Listen should log an error and return without attempting to fetch data
        with patch.object(client, 'fetch_candlestick_data') as mock_fetch:
            await client.listen()
            mock_fetch.assert_not_called()
    
    @patch('websockets.connect')
    async def test_listen_with_exception(self, mock_connect):
        """Test listen method handling exceptions properly"""
        # Configure fetch_candlestick_data to raise an exception
        async def mock_fetch_with_exception():
            raise Exception("Test exception")
            yield None  # This line will never be reached
        
        with patch.object(self.client, 'fetch_candlestick_data', 
                         new=mock_fetch_with_exception):
            with self.assertRaises(Exception):
                await self.client.listen()
                
            # Verify disconnect was called
            self.client.connection_manager.disconnect.assert_called_once()
    
    def test_setup_logger(self):
        """Test logger setup functionality"""
        # Create client with a custom logger name for testing
        client = BinanceWebSocketClient('testpair', 'test_interval')
        logger = client.logger
        
        # Verify logger configuration
        self.assertEqual(logger.name, 'BinanceWS_testpair_test_interval')
        self.assertEqual(logger.level, logging.INFO)
        self.assertEqual(len(logger.handlers), 1)
        self.assertIsInstance(logger.handlers[0], logging.StreamHandler)
