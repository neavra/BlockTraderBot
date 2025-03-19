# monitoring_layer/core/alerts/alert_manager.py
import asyncio
import logging
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum

from shared.dto.alert import Alert  # Using your existing Alert class

logger = logging.getLogger(__name__)

class AlertProvider(ABC):
    """Abstract base class for alert providers."""
    
    @abstractmethod
    async def send_alert(self, alert: Alert) -> bool:
        """
        Send an alert through this provider.
        
        Args:
            alert: Alert object containing all information
            
        Returns:
            True if alert was sent successfully, False otherwise
        """
        pass

class TelegramAlertProvider(AlertProvider):
    """Telegram implementation of the alert provider."""
    
    def __init__(self, telegram_bot):
        """
        Initialize the Telegram alert provider.
        
        Args:
            telegram_bot: Instance of TelegramBot
        """
        self.telegram_bot = telegram_bot
    
    async def send_alert(self, alert: Alert) -> bool:
        """
        Send an alert via Telegram.
        
        Args:
            alert: Alert object containing all information
            
        Returns:
            True if alert was sent successfully, False otherwise
        """
        try:
            # Use the existing telegram bot's send_alert method
            await self.telegram_bot.send_alert(alert)
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {str(e)}")
            return False

class AlertManager:
    """
    Alert manager that handles sending alerts through multiple providers.
    """
    
    def __init__(self, providers: List[AlertProvider] = None):
        """
        Initialize the alert manager.
        
        Args:
            providers: List of alert providers to use
        """
        self.providers = providers or []
        self.alert_history = []
        self.max_history = 100  # Maximum number of alerts to keep in history
    
    def add_provider(self, provider: AlertProvider):
        """
        Add an alert provider.
        
        Args:
            provider: Alert provider to add
        """
        if provider not in self.providers:
            self.providers.append(provider)
    
    def remove_provider(self, provider: AlertProvider):
        """
        Remove an alert provider.
        
        Args:
            provider: Alert provider to remove
        """
        if provider in self.providers:
            self.providers.remove(provider)
    
    async def send_alert(self, alert: Alert) -> bool:
        """
        Send an alert through all configured providers.
        
        Args:
            alert: Alert object containing all information
            
        Returns:
            True if at least one provider successfully sent the alert
        """
        if not self.providers:
            logger.warning("No alert providers configured")
            return False
        
        # Record the alert in history
        self._add_to_history(alert)
        
        # Send through all providers
        results = await asyncio.gather(
            *[provider.send_alert(alert) for provider in self.providers],
            return_exceptions=True
        )
        
        # Check if at least one provider succeeded
        success = any(
            isinstance(result, bool) and result is True 
            for result in results
        )
        
        if not success:
            logger.error(f"Failed to send alert through any provider: {alert.message}")
        
        return success
    
    def _add_to_history(self, alert: Alert):
        """Add an alert to the history, maintaining the maximum size."""
        self.alert_history.append(alert)
        
        # Trim history if it exceeds the maximum size
        if len(self.alert_history) > self.max_history:
            self.alert_history = self.alert_history[-self.max_history:]
    
    def get_recent_alerts(self, limit: int = 10, alert_type: Optional[str] = None) -> List[Alert]:
        """
        Get recent alerts from history.
        
        Args:
            limit: Maximum number of alerts to return
            alert_type: Optional type to filter by
            
        Returns:
            List of recent alert records
        """
        if alert_type:
            filtered = [a for a in self.alert_history if a.type.name == alert_type]
            return filtered[-limit:]
        else:
            return self.alert_history[-limit:]