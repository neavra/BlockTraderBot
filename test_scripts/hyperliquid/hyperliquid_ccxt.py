import ccxt
import os
from dotenv import load_dotenv
import json

# Load environment variables from .env file (create this file with your keys)
load_dotenv()

def create_hyperliquid_order():
    # Get API credentials from environment variables for security
    api_key = os.getenv('HYPERLIQUID_API_KEY')
    api_secret = os.getenv('HYPERLIQUID_API_SECRET')
    
    if not api_key or not api_secret:
        print("Error: API credentials not found. Please set HYPERLIQUID_API_KEY and HYPERLIQUID_API_SECRET in your .env file.")
        return
    
    # Initialize exchange
    exchange = ccxt.hyperliquid({
        'apiKey': api_key,
        'secret': api_secret,
        # Add additional required authentication params if needed
    })
    
    # Print available markets for reference
    markets = exchange.load_markets()
    print(f"Available markets: {', '.join(list(markets.keys())[:10])}...")
    
    # Order parameters - MODIFY THESE VALUES
    symbol = 'BTC/USD'  # Exchange-specific trading pair format
    order_type = 'limit'  # 'market' or 'limit'
    side = 'buy'  # 'buy' or 'sell'
    amount = 0.01  # Trading amount in base currency
    price = 65000  # Price for limit orders (can be None for market orders)
    
    # Additional parameters
    params = {
        'timeInForce': 'Gtc',  # Good-till-canceled
        'postOnly': False,
        'reduceOnly': False,
        # 'clientOrderId': '0x1234567890abcdef1234567890abcdef', # Optional
        # 'slippage': '0.001',  # Optional for market orders
    }
    
    try:
        # Print order details before submission
        print("\nPreparing to submit order:")
        print(f"Symbol: {symbol}")
        print(f"Type: {order_type}")
        print(f"Side: {side}")
        print(f"Amount: {amount}")
        print(f"Price: {price}")
        print(f"Additional params: {json.dumps(params, indent=2)}")
        
        # Uncomment to actually place the order
        # order = exchange.create_order(symbol, order_type, side, amount, price, params)
        # print(f"\nOrder placed successfully: {json.dumps(order, indent=2)}")
        
        print("\nThis is a simulation - no actual order was placed.")
        print("To place a real order, uncomment the relevant code section and provide valid API credentials.")
        
    except Exception as e:
        print(f"\nError creating order: {e}")
        
        # Print more detailed error information if available
        if hasattr(e, 'args') and len(e.args) > 0:
            print(f"Details: {e.args[0]}")

if __name__ == "__main__":
    create_hyperliquid_order()