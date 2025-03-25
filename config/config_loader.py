import os
import json
from pathlib import Path
from dotenv import load_dotenv

def load_config():
    """
    Load configuration from JSON file and environment variables
    """
    # Load environment variables
    load_dotenv()
    
    # Determine environment
    env = os.getenv('ENVIRONMENT', 'development')
    
    # Load default config
    config_dir = Path(__file__).parent
    config_path = config_dir / 'default_config.json'
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Load environment-specific config if it exists
    env_config_path = config_dir / f'{env}_config.json'
    if env_config_path.exists():
        with open(env_config_path, 'r') as f:
            env_config = json.load(f)
            # Merge configs (simple deep merge)
            deep_merge(config, env_config)
    
    # Add sensitive data from environment variables
    if 'exchanges' not in config:
        config['exchanges'] = {}
    
    if 'hyperliquid' not in config['exchanges']:
        config['exchanges']['hyperliquid'] = {}

    if 'database' not in config['data']:
        config['data']['database'] = {}
    
    if 'cache' not in config['data']:
        config['data']['cache'] = {}

    if 'queue' not in config['data']:
        config['data']['queue'] = {}

    
    # Add sensitive credentials from environment variables
    config['exchanges']['hyperliquid'].update({
        'wallet_address': os.getenv('HL_WALLET_ADDRESS', ''),
        'private_key': os.getenv('HL_PRIVATE_KEY', '')
    })
    config['monitoring']['telegram'].update({
        'bot_token': os.getenv('BOT_TOKEN', ''),
        'chat_id': os.getenv('CHAT_ID', '')
    })
    config['data']['database'].update({
        'database_url' : os.environ.get("DATABASE_URL", ""),
        'postgres_user': os.environ.get("POSTGRES_USER", ""),
        'postgres_password' : os.environ.get("POSTGRES_PASSWORD", ""),
        'postgres_db' : os.environ.get("POSTGRES_DB", "")
    })
    config['data']['cache'].update({
        'redis_url' : os.environ.get("REDIS_URL", ""),
        'redis_password': os.environ.get("REDIS_PASSWORD", ""),
    })
    config['data']['queue'].update({
        'event_bus_url' : os.environ.get("EVENT_BUS_URL", ""),
    })
    
    return config

def deep_merge(dict1, dict2):
    """
    Deep merge two dictionaries
    """
    for key in dict2:
        if key in dict1 and isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
            deep_merge(dict1[key], dict2[key])
        else:
            dict1[key] = dict2[key]