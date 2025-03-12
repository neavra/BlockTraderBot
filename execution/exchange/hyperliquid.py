import ccxt.async_support as ccxt
from typing import Dict, List, Optional, Any
import logging

from execution.exchange.exchange_interface import ExchangeInterface

class HyperliquidExchange(ExchangeInterface):
    @property
    def id(self) -> str:
        """Exchange identifier"""
        #TODO Consider using ENUM here instead
        return "hyperliquid"
    
    @property
    def name(self) -> str:
        """Exchange name"""
        return "Hyperliquid"
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the Hyperliquid exchange with configuration"""
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.exchange = ccxt.hyperliquid({
            'walletAddress': config.get('wallet_address'),
            'privateKey': config.get('private_key'),
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',
                'adjustForTimeDifference': True,
                'testnet': True,
            }
        })
    
    """
        Initialize the Hyperliquid exchange connection.
        This method:
        1. Loads markets to verify connectivity
        2. Returns success/failure status
    """
    async def initialize(self) -> bool:
        
        try:
            self.logger.info("Initializing Hyperliquid exchange connection...")
            
            # For simulation, we'll skip actual API calls and simulate success
            self.logger.info("Loading markets...")
            # Comment out actual API call for testing
            markets = await self.exchange.load_markets(reload=True)
            
            self.logger.info(f"Loaded {len(markets)} markets")
                
            self.logger.info("Hyperliquid exchange initialization complete")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during initialization: {e}")
            return False
    
    """
        Create a new order on the exchange.
        
        Args:
            symbol: Trading pair symbol
            order_type: Order type (market, limit, etc.)
            side: Order side (buy or sell)
            amount: Order amount in base currency
            price: Order price (required for limit orders)
            params: Additional parameters
            
        Returns:
            Order information dictionary
    """
    async def create_order(self, 
                     symbol: str, 
                     order_type: str, 
                     side: str, 
                     amount: float, 
                     price: Optional[float] = None, 
                     params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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

    """
        Cancel an existing order.
        
        Args:
            id: Order ID
            symbol: Trading pair (required for Hyperliquid)
            params: Additional exchange-specific parameters
            
        Returns:
            Dict containing cancellation details
    """
    async def cancel_order(self, id: str, symbol: Optional[str] = None, 
                         params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        
        try:
            if symbol is None:
                raise ValueError("Symbol is required for canceling orders on Hyperliquid")
                
            self.logger.info(f"Cancelling order {id} for {symbol}")
            
            # Cancel the order
            result = await self.exchange.cancel_order(id, symbol, params or {})
            
            self.logger.info(f"Order {id} cancelled successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Error cancelling order {id}: {e}")
            raise
    
    """
        Fetch an order's status.
        
        Args:
            id: Order ID
            symbol: Trading pair (required for Hyperliquid)
            params: Additional exchange-specific parameters
            
        Returns:
            Dict containing order details including status
    """
    async def fetch_order(self, id: str, symbol: Optional[str] = None, 
                        params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            if symbol is None:
                raise ValueError("Symbol is required for fetching orders on Hyperliquid")
                
            self.logger.info(f"Fetching order {id} for {symbol}")
            
            # Fetch the order
            order = await self.exchange.fetch_order(id, symbol, params or {})
            
            return order
            
        except Exception as e:
            self.logger.error(f"Error fetching order {id}: {e}")
            raise
    
    """
        Fetch all open orders.
        
        Args:
            symbol: Trading pair to filter by
            since: Timestamp to fetch orders from
            limit: Maximum number of orders to fetch
            params: Additional exchange-specific parameters
            
        Returns:
            List of order details
    """
    async def fetch_open_orders(self, symbol: Optional[str] = None, 
                              since: Optional[int] = None, 
                              limit: Optional[int] = None, 
                              params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            custom_params = params or {}
            
            # Add since and limit to params if provided
            if since is not None:
                custom_params['since'] = since
            if limit is not None:
                custom_params['limit'] = limit
                
            self.logger.info(f"Fetching open orders for {'all symbols' if symbol is None else symbol}")
            
            # Fetch open orders
            open_orders = await self.exchange.fetch_open_orders(symbol, since, limit, custom_params)
            
            return open_orders
            
        except Exception as e:
            self.logger.error(f"Error fetching open orders: {e}")
            raise
    
    """
        Fetch positions (for margin/futures markets).
        
        Args:
            symbols: List of trading pairs to fetch positions for
            params: Additional exchange-specific parameters
            
        Returns:
            List of position details
    """
    async def fetch_positions(self, symbols: Optional[List[str]] = None, 
                            params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        
        try:
            self.logger.info(f"Fetching positions for {symbols if symbols else 'all symbols'}")
            
            # Check if the exchange supports fetch_positions
            if hasattr(self.exchange, 'fetch_positions'):
                positions = await self.exchange.fetch_positions(symbols, params or {})
                return positions
            
            # Fallback for exchanges without dedicated positions endpoint
            # Try to extract position information from balance
            balance = await self.fetch_balance()
            
            # Extract positions from the balance info (implementation depends on exchange)
            # For Hyperliquid, positions might be in balance['info']['positions']
            positions = []
            if 'info' in balance and 'positions' in balance['info']:
                raw_positions = balance['info']['positions']
                
                for pos in raw_positions:
                    if 'symbol' in pos and (symbols is None or pos['symbol'] in symbols):
                        positions.append({
                            'symbol': pos['symbol'],
                            'side': 'long' if float(pos.get('positionAmt', 0)) > 0 else 'short',
                            'size': abs(float(pos.get('positionAmt', 0))),
                            'notional': float(pos.get('notional', 0)),
                            'leverage': float(pos.get('leverage', 1)),
                            'entryPrice': float(pos.get('entryPrice', 0)),
                            'unrealizedPnl': float(pos.get('unrealizedProfit', 0)),
                            'info': pos
                        })
            
            return positions
            
        except Exception as e:
            self.logger.error(f"Error fetching positions: {e}")
            raise
    
    """
        Fetch account balance.
        
        Args:
            params: Additional exchange-specific parameters
            
        Returns:
            Dict containing balance information
    """
    async def fetch_balance(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            self.logger.info("Fetching account balance")
            balance = await self.exchange.fetch_balance(params or {})
            
            return balance
            
        except Exception as e:
            self.logger.error(f"Error fetching balance: {e}")
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