import sys
import asyncio
from pathlib import Path
import logging
import queue
import threading
import time
from datetime import datetime

from alert.telegram_bot import TelegramBot
from shared.dto.alert import AlertType
from monitoring_service import MonitoringService

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Add the project root to the Python path if running this file directly
if __name__ == "__main__":
    # Get the absolute path of the current script
    current_dir = Path(__file__).resolve().parent
    # Add the parent directory (project root) to sys.path
    project_root = current_dir.parent
    sys.path.append(str(project_root))

from config.config_loader import load_config

def run_monitoring_layer():
    """Main entry point for the application."""
    config = load_config()
    telegram_config = config['monitoring']['telegram']
    # Replace with your actual Telegram bot token and chat ID
    BOT_TOKEN = telegram_config.get('bot_token')
    CHAT_ID = telegram_config.get('chat_id')
    
    # Initialize the Telegram bot
    bot = TelegramBot(token=BOT_TOKEN, chat_id=CHAT_ID)
    
    # Create an event queue
    event_queue = queue.Queue()
    
    # Initialize the monitoring service
    monitoring_service = MonitoringService(telegram_bot=bot, event_queue=event_queue)
    
    # Start the monitoring service
    monitoring_service.start()
    
    # Start the Telegram bot in a separate thread
    bot_thread = threading.Thread(target=bot.start, daemon=True)
    bot_thread.start()
    
    logger.info("Application started. Press Ctrl+C to stop.")
    
    # Example: Add a test event once everything is running
    time.sleep(2)  # Give services time to initialize
    monitoring_service.add_event({
        "type": "INFO",
        "symbol": "SYSTEM",
        "message": "Trading system started and ready",
        "timestamp": datetime.now().isoformat(),
        "details": {
            "Status": "Running",
            "Version": "1.0.0"
        }
    })
    
    # Keep the main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down application...")
        monitoring_service.stop()
        logger.info("Application stopped.")

if __name__ == "__main__":
    run_monitoring_layer()