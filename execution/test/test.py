import os
import sys
import asyncio
from pathlib import Path

# Add the project root to the Python path if running this file directly
if __name__ == "__main__":
    # Get the absolute path of the current script
    current_dir = Path(__file__).resolve().parent
    # Add the parent directory (project root) to sys.path
    project_root = current_dir.parent
    sys.path.append(str(project_root))

# Now you can import from the config package
from config.config_loader import load_config
from execution.exchange.hyperliquid import HyperliquidExchange
# from execution.exchange.exchange_executor import ExchangeExecutor

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

async def run_execution_layer():
    """Run the execution layer in isolation"""
    print("Starting execution layer...")
    
    # Load the full config
    config = load_config()
    
    # Extract execution-specific config
    exchange_config = config['exchanges']['hyperliquid']
    test_run = config.get('execution', {}).get('test_run', False)
    # risk_config = config['risk_settings']
    
    # Create and initialize exchange
    exchange = HyperliquidExchange(exchange_config)
    
    if not await exchange.initialize():
        print("Failed to initialize exchange connection")
        return
    
    # # Create executor with combined config
    # executor_config = {
    #     'risk_settings': risk_config,
    #     # Add other configs needed by executor
    # }
    # executor = ExchangeExecutor(exchange, executor_config)
    
    # Example: Test exchange by fetching balance
    try:
        balance = await exchange.fetch_balance()
        print(f"Account balance: {balance}")
        if test_run:
            await run_all_tests(exchange)
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        # Close exchange connection
        await exchange.close()

if __name__ == "__main__":
    # Run the execution layer
    asyncio.run(run_execution_layer())