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
from execution.test import run_all_tests
# from execution.exchange.exchange_executor import ExchangeExecutor

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