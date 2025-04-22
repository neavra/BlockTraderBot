import logging
from typing import Dict, Any, Optional, List
from strategy.context.analyzers.base import BaseAnalyzer
from strategy.context.analyzers.swing_detector import SwingDetector
from strategy.context.analyzers.trend_analyzer import TrendAnalyzer
from strategy.context.analyzers.range_detector import RangeDetector

logger = logging.getLogger(__name__)

class AnalyzerFactory:
    """Factory for creating market analyzers"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize with optional configuration
        
        Args:
            config: Configuration dictionary for analyzers
        """
        self.config = config or {}
        self.analyzers = {}
    
    def create_analyzer(self, analyzer_type: str, **kwargs) -> Optional[BaseAnalyzer]:
        """
        Create an analyzer of the specified type
        
        Args:
            analyzer_type: Type of analyzer to create
            **kwargs: Additional parameters to pass to the analyzer
            
        Returns:
            Initialized analyzer instance or None if type is invalid
        """
        # Get config for this analyzer type
        analyzer_config = self.config.get(analyzer_type, {})
        
        # Merge with kwargs (kwargs take precedence)
        params = {**analyzer_config, **kwargs}
        
        try:
            # Import the appropriate analyzer class
            if analyzer_type == "swing":
                return SwingDetector(**params)
            elif analyzer_type == "trend":
                return TrendAnalyzer(**params)
            elif analyzer_type == "range":
                return RangeDetector(**params)
            else:
                logger.warning(f"Unknown analyzer type: {analyzer_type}")
                return None
        except Exception as e:
            logger.error(f"Error creating analyzer {analyzer_type}: {e}")
            return None
    
    def create_default_analyzers(self) -> Dict[str, BaseAnalyzer]:
        """
        Create a set of default analyzers
        
        Returns:
            Dictionary of analyzer instances by type
        """
        analyzers = {}
        
        # Create standard analyzers
        for analyzer_type in ["swing", "trend", "range", "fibbonacci"]:
            analyzer = self.create_analyzer(analyzer_type)
            if analyzer:
                analyzers[analyzer_type] = analyzer
                self.analyzers[analyzer_type] = analyzer
        
        return analyzers
    
    def get_analyzer(self, analyzer_type: str) -> Optional[BaseAnalyzer]:
        """
        Get an existing analyzer by type
        
        Args:
            analyzer_type: Type of analyzer to retrieve
            
        Returns:
            Analyzer instance or None if not found
        """
        return self.analyzers.get(analyzer_type)
    
    def get_enabled_analyzers(self) -> List[BaseAnalyzer]:
        """
        Get all enabled analyzers
        
        Returns:
            List of enabled analyzer instances
        """
        return list(self.analyzers.values())
