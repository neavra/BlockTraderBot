# config/settings.py
import os
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.
# List of trading pairs
SYMBOLS = ["BTCUSDT", "ETHUSDT"]  
# List of timeframes
#TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]  
TIMEFRAMES = [ "1m", "5m", "15m"]  

EXCHANGES = [
    {
        "name": "binance",
        "symbols": SYMBOLS,
        "timeframes": TIMEFRAMES
    }
]  # Define exchanges with their symbols and timeframes

class Config:
    DATABASE_URL : str = os.environ.get("DATABASE_URL")
    POSTGRES_USER: str = os.environ.get("POSTGRES_USER")
    POSTGRES_PASSWORD : str = os.environ.get("POSTGRES_PASSWORD")
    POSTGRES_DB : str = os.environ.get("POSTGRES_DB")
    REDIS_URL : str = os.environ.get("REDIS_URL")
    REDIS_PASSWORD : str = os.environ.get("REDIS_PASSWORD")
    EVENT_BUS_URL: str = os.environ.get("EVENT_BUS_URL")

config = Config()