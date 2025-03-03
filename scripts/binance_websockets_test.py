import json
import websocket
import datetime
import time
import ssl

# Define callback functions for WebSocket events
def on_message(ws, message):
    """Handle incoming messages from the WebSocket connection"""
    # Parse the JSON data
    data = json.loads(message)
    
    # Extract the kline (candlestick) data
    if 'k' in data:
        kline = data['k']
        
        # Create a more readable format for the candlestick data
        candle = {
            'symbol': kline['s'],
            'interval': kline['i'],
            'open_time': datetime.datetime.fromtimestamp(kline['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            'open': float(kline['o']),
            'high': float(kline['h']),
            'low': float(kline['l']),
            'close': float(kline['c']),
            'volume': float(kline['v']),
            'closed': kline['x']  # Whether the candle is closed (completed)
        }
        
        # Print the formatted candle data
        print(f"Symbol: {candle['symbol']} | Interval: {candle['interval']} | Time: {candle['open_time']}")
        print(f"OHLC: {candle['open']} / {candle['high']} / {candle['low']} / {candle['close']}")
        print(f"Volume: {candle['volume']} | Closed: {candle['closed']}")
        print("-" * 50)

def on_error(ws, error):
    """Handle WebSocket errors"""
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket connection close"""
    print(f"Connection closed: {close_status_code} - {close_msg}")

def on_open(ws):
    """Handle WebSocket connection open"""
    print("Connection established!")
    
    # Create a subscription message
    # For example, subscribe to BTC/USDT 1-minute candlestick data
    subscribe_msg = {
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@kline_1m"  # Format: <symbol>@kline_<interval>
        ],
        "id": 1
    }
    
    # Send the subscription message
    ws.send(json.dumps(subscribe_msg))
    print("Subscribed to BTCUSDT 1-minute candlestick stream")

def start_binance_websocket(symbol="btcusdt", interval="1m"):
    """
    Start a WebSocket connection to Binance for candlestick data
    
    Parameters:
    - symbol: Trading pair (lowercase, e.g., 'btcusdt')
    - interval: Candlestick interval (e.g., '1m', '5m', '15m', '1h', '4h', '1d')
    """
    # Construct the WebSocket URL
    socket_url = f"wss://stream.binance.com:9443/ws"
    
    # Set up the WebSocket with callback functions
    ws = websocket.WebSocketApp(
        socket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Enable trace for debugging (optional)
    websocket.enableTrace(False)
    
    # Start the WebSocket connection (this will run forever until interrupted)
    print(f"Connecting to Binance WebSocket for {symbol.upper()} {interval} candlesticks...")
    
    # Add this line to bypass SSL verification
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

if __name__ == "__main__":
    # Make sure to install the required packages:
    # pip install websocket-client
    
    # You can change the symbol and interval here
    start_binance_websocket(symbol="btcusdt", interval="1m")
    
    # Note: This script will run until interrupted (Ctrl+C)
    # The WebSocket will continuously receive data until then