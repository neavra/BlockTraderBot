import pytest
import aiohttp
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timezone

from connectors.rest.binance_rest import BinanceRestClient
from domain.models.candle import CandleData


@pytest.fixture
def client():
    """Create a client instance for testing."""
    return BinanceRestClient(symbol="BTCUSDT", interval="1m")


class TestBinanceRestClient:
    """Test cases for BinanceRestClient class."""

    def test_init(self):
        """Test client initialization with default and custom parameters."""
        # Default initialization
        client = BinanceRestClient(symbol="btcusdt", interval="1h")
        assert client.symbol == "BTCUSDT"  # Should convert to uppercase
        assert client.interval == "1h"
        assert client.exchange == "binance"
        assert "api.binance.com" in client.base_url

        # Custom base URL
        custom_url = "https://testnet.binance.vision/api/v3/klines"
        client = BinanceRestClient(symbol="ETHUSDT", interval="5m", base_url=custom_url)
        assert client.base_url == custom_url
        assert client.symbol == "ETHUSDT"
        assert client.interval == "5m"

    def test_build_url(self, client):
        """Test URL building with various parameters."""
        # Basic URL without optional parameters
        url = client._build_url()
        assert "symbol=BTCUSDT" in url
        assert "interval=1m" in url
        assert "limit=" not in url
        assert "startTime=" not in url
        assert "endTime=" not in url

        # URL with all parameters
        url = client._build_url(limit=100, startTime=1609459200000, endTime=1609545600000)
        assert "symbol=BTCUSDT" in url
        assert "interval=1m" in url
        assert "limit=100" in url
        assert "startTime=1609459200000" in url
        assert "endTime=1609545600000" in url

        # URL with some parameters
        url = client._build_url(limit=500)
        assert "symbol=BTCUSDT" in url
        assert "interval=1m" in url
        assert "limit=500" in url
        assert "startTime=" not in url
        assert "endTime=" not in url

    @pytest.mark.asyncio
    async def test_fetch_candlestick_data_success(self, client):
        """Test successful API call to fetch candlestick data."""
        # Mock response data
        mock_response = [
            [1609459200000, "29000.00", "29100.00", "28900.00", "29050.00", "100.5", 1609462800000, "2915225.00", 100, "50.5", "1457612.50", "0"]
        ]
        
        # Set up the mock response properly for aiohttp
        mock_response_context = MagicMock()
        mock_response_context.__aenter__ = AsyncMock(return_value=mock_response_context)
        mock_response_context.__aexit__ = AsyncMock(return_value=None)
        mock_response_context.status = 200
        mock_response_context.json = AsyncMock(return_value=mock_response)
        
        # Set up the session mock
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = MagicMock(return_value=mock_response_context)
        
        # Patch the ClientSession to return our mock
        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await client.fetch_candlestick_data(limit=1)
            
            # Check the result
            assert result == mock_response
            assert len(result) == 1
            
            # Verify the session was called with correct URL
            called_url = mock_session.get.call_args[0][0]
            assert "symbol=BTCUSDT" in called_url
            assert "interval=1m" in called_url
            assert "limit=1" in called_url

    @pytest.mark.asyncio
    async def test_fetch_candlestick_data_api_error(self, client):
        """Test handling of API errors."""
        # Set up the mock response with error
        mock_response_context = MagicMock()
        mock_response_context.__aenter__ = AsyncMock(return_value=mock_response_context)
        mock_response_context.__aexit__ = AsyncMock(return_value=None)
        mock_response_context.status = 400
        mock_response_context.text = AsyncMock(return_value='{"code":-1121,"msg":"Invalid symbol."}')
        
        # Set up the session mock
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = MagicMock(return_value=mock_response_context)
        
        # Patch the ClientSession to return our mock
        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await client.fetch_candlestick_data()
            
            # Should return empty list on error
            assert result == []

    @pytest.mark.asyncio
    async def test_fetch_candlestick_data_exception(self, client):
        """Test handling of exceptions during API call."""
        # Instead of making the session.get raise an exception,
        # we'll make the overall try/except block catch an exception
        with patch('aiohttp.ClientSession') as mock_session_class:
            # Mock session to raise exception when used
            mock_session_class.side_effect = Exception("Connection error")
            
            # The method should catch the exception and return an empty list
            result = await client.fetch_candlestick_data()
            assert result == []

    @pytest.mark.asyncio
    async def test_fetch_candlestick_data_validation_exception_handling(self, client):
        """Test that invalid parameters are caught by the try/except block in the method."""
        # Since the method has a try/except that catches all exceptions,
        # we need to verify that the validation check happens but is caught
        
        # Create a logger mock to verify errors are logged
        with patch.object(client, 'logger') as mock_logger:
            # Test with invalid startTime
            result = await client.fetch_candlestick_data(startTime="invalid")
            assert result == []
            # Verify an error was logged
            mock_logger.error.assert_called()
            
            # Reset the mock
            mock_logger.reset_mock()
            
            # Test with invalid endTime
            result = await client.fetch_candlestick_data(endTime="invalid")
            assert result == []
            # Verify an error was logged
            mock_logger.error.assert_called()