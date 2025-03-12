import asyncio
import logging
from typing import Dict, Any
from hyperliquid_exchange import HyperliquidExchange

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HyperliquidMain")

async def main():
    # Exchange configuration
    config = {
        # TESTING PRIVATE KEY DO NOT USE WITH FUNDS
        'wallet_address': '0x0d6aee6775a657f0E38c84620974A2A09FeB7f9c',
        'private_key': '381e79809646559c001d0fd0e7216f320ca935ac4c8aea9ba1f6f3454570510d',
        'leverage': 5,
        'margin_mode': 'cross'
    }
    
    # Create and initialize exchange
    logger.info("Creating Hyperliquid exchange instance")
    exchange = HyperliquidExchange(config)
    
    try:
        # Initialize the exchange
        logger.info("Initializing exchange...")
        success = await exchange.initialize()
        
        if not success:
            logger.error("Failed to initialize exchange")
            return
            
        logger.info("Exchange initialized successfully")
        
        # Order parameters
        symbol = "BTC/USDC:USDC"
        order_type = "limit"
        side = "buy"
        amount = 0.01    # Order quantity
        price = 50000.0  # Limit price
        
        # Additional parameters
        params = {
            'clientOrderId': f'test_order_{int(asyncio.get_event_loop().time())}',
            'timeInForce': 'GTC'  # Good Till Canceled
        }
        
        # Place the order
        logger.info(f"Placing {order_type} {side} order: {amount} {symbol} @ ${price}")
        
        order = await exchange.create_order(
            symbol=symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price,
            params=params
        )
        
        # Display order result
        logger.info(f"Order placed successfully!")
        logger.info(f"Order ID: {order['id']}")
        logger.info(f"Status: {order['status']}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
    
    finally:
        # Always close the exchange connection
        if exchange is not None:
            logger.info("Closing exchange connection")
            await exchange.close()
            logger.info("Exchange connection closed")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())