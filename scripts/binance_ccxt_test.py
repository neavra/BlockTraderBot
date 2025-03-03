import ccxt
import pandas as pd
from datetime import datetime
import time

def fetch_historical_ohlcv(symbol, timeframe, limit=100):
    """
    Fetch historical OHLCV data using CCXT
    
    Parameters:
    - symbol: Trading pair (e.g., 'BTC/USDT')
    - timeframe: Candle timeframe (e.g., '1m', '5m', '1h')
    - limit: Number of candles to fetch
    
    Returns:
    - Pandas DataFrame with OHLCV data
    """
    # Initialize Binance exchange
    exchange = ccxt.binance()
    
    # Fetch OHLCV data
    print(f"Fetching {limit} {timeframe} candles for {symbol}...")
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    
    # Convert to DataFrame
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    return df

if __name__ == "__main__":
    try:
        # Get historical data for BTC/USDT 1-minute timeframe (last 30 candles)
        df = fetch_historical_ohlcv('BTC/USDT', '1m', limit=30)
        
        # Display the data
        print("\nHistorical BTC/USDT 1-minute Data:")
        print("==================================")
        print(df)
        
        # Display some basic statistics
        print("\nBasic Statistics:")
        print("================")
        print(f"Average price: ${df['close'].mean():.2f}")
        print(f"Highest price: ${df['high'].max():.2f}")
        print(f"Lowest price: ${df['low'].min():.2f}")
        print(f"Total volume: {df['volume'].sum():.2f} BTC")
        
        # Calculate price change percentage
        price_change = ((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]) * 100
        print(f"Price change over period: {price_change:.2f}%")
        
        # Time range
        print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
    except Exception as e:
        print(f"Error: {e}")