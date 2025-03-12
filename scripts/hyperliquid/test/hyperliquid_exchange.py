import ccxt.async_support as ccxt
from typing import Dict, List, Optional, Any
import logging
import asyncio
import time
import uuid

from exchange_interface import ExchangeInterface

class HyperliquidExchange(ExchangeInterface):
    def __init__(self, config: Dict[str, Any]):
        """Initialize the Hyperliquid exchange with configuration"""
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.exchange = ccxt.hyperliquid({
            'walletAddress': config.get('wallet_address'),
            'privateKey': config.get('private_key'),
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',  # For perpetual futures
                'adjustForTimeDifference': True,
                'testnet': True,
            }
        })
    
    async def initialize(self) -> bool:
        """
        Initialize the Hyperliquid exchange connection.
        This method:
        1. Loads markets to verify connectivity
        2. Returns success/failure status
        """
        try:
            self.logger.info("Initializing Hyperliquid exchange connection...")
            
            # For simulation, we'll skip actual API calls and simulate success
            self.logger.info("[SIMULATION] Loading markets...")
            # Comment out actual API call for testing
            markets = await self.exchange.load_markets(reload=True)
            
            self.logger.info(f"Loaded {len(markets)} markets")
                
            self.logger.info("Hyperliquid exchange initialization complete")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during initialization: {e}")
            return False
    
    async def create_order(self, 
                     symbol: str, 
                     order_type: str, 
                     side: str, 
                     amount: float, 
                     price: Optional[float] = None, 
                     params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create an order on Hyperliquid (simplified for testing)
        """
        try:
            # Prepare parameters
            order_type = order_type.lower()
            custom_params = params or {}
            
            # Log the order
            self.logger.info(f"Creating {order_type} {side} order for {symbol}: {amount} @ {price}")
            
            # Simple parameter handling for client order ID
            if 'clientOrderId' in custom_params:
                custom_params['clOrdId'] = custom_params.pop('clientOrderId')
            
            # Create the order
            result = await self.exchange.create_order(
                symbol, order_type, side, amount, price, custom_params
            )
            
            self.logger.info(f"Order created successfully: {result['id']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error creating order: {e}")
            raise

    async def close(self) -> None:
        """Close the exchange connection and release resources"""
        try:
            if self.exchange:
                self.logger.info("Closing Hyperliquid exchange connection")
                await self.exchange.close()
                self.logger.info("Exchange connection closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing exchange connection: {e}")