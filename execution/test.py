import asyncio
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def test_balance(exchange):
    """Test fetching balance"""
    try:
        print("\n=== Testing Balance ===")
        balance = await exchange.fetch_balance()
        print(f"Account balance: {balance}")
        return True
    except Exception as e:
        print(f"Error fetching balance: {e}")
        return False

async def test_markets(exchange):
    """Test fetching markets/symbols"""
    try:
        print("\n=== Testing Markets ===")
        # Since we're using CCXT, we need to access the underlying markets
        # This isn't in our interface but useful for testing
        markets = exchange.exchange.markets
        print(f"Available markets: {len(markets)}")
        print(f"Sample markets: {list(markets.keys())[:5]}")
        return True
    except Exception as e:
        print(f"Error fetching markets: {e}")
        return False

async def test_open_orders(exchange):
    """Test fetching open orders"""
    try:
        print("\n=== Testing Open Orders ===")
        orders = await exchange.fetch_open_orders()
        print(f"Open orders: {len(orders)}")
        if orders:
            print(f"Sample order: {orders[0]}")
        return True
    except Exception as e:
        print(f"Error fetching open orders: {e}")
        return False

async def test_positions(exchange):
    """Test fetching positions"""
    try:
        print("\n=== Testing Positions ===")
        positions = await exchange.fetch_positions()
        print(f"Open positions: {len(positions)}")
        if positions:
            print(f"Sample position: {positions[0]}")
        return True
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return False

async def test_create_order(exchange, symbol):
    """Test creating and canceling a small test order"""
    try:
        print(f"\n=== Testing Order Creation for {symbol} ===")
        # Create a very small limit order far from market price to avoid execution
        # For testing only - use a tiny amount and a price unlikely to be hit
        
        # Get the current ticker to find a safe price
        ticker = await exchange.exchange.fetch_ticker(symbol)
        current_price = ticker['last']
        
        # Set limit buy 20% below current price
        test_price = current_price * 0.8
        test_amount = 0.001  # Very small amount
        
        print(f"Creating test limit buy order: {test_amount} {symbol} @ {test_price}")
        
        order = await exchange.create_order(
            symbol=symbol,
            order_type='limit',
            side='buy',
            amount=test_amount,
            price=test_price,
            params={'timeInForce': 'GTC'}
        )
        
        print(f"Test order created: {order['id']}")
        
        # Fetch the order to confirm it exists
        fetched_order = await exchange.fetch_order(order['id'], symbol)
        print(f"Fetched order status: {fetched_order['status']}")
        
        # Cancel the test order
        print(f"Cancelling test order {order['id']}")
        cancel_result = await exchange.cancel_order(order['id'], symbol)
        print(f"Order cancellation result: {cancel_result}")
        
        return True
    except Exception as e:
        print(f"Error in order test: {e}")
        return False

async def run_all_tests(exchange):
    print("\n🧪 TEST RUN ENABLED - Running additional tests 🧪")    
    # Run various tests
    await test_markets(exchange)
    await test_open_orders(exchange)
    await test_positions(exchange)
    test_symbol = "BTC/USDC:USDC"        
    await test_create_order(exchange, test_symbol)