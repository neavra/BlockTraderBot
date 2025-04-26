import logging
from typing import Dict, Any, Optional, List
from strategy.context.analyzers.base import BaseAnalyzer
from strategy.context.analyzers.swing_detector import SwingDetector
from strategy.context.analyzers.trend_analyzer import TrendAnalyzer
from strategy.context.analyzers.range_detector import RangeDetector
from strategy.context.analyzers.fibbonacci_analyzer import FibonacciAnalyzer

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
        # Get config for this specific analyzer type
        analyzer_config = self.config.get(analyzer_type, {})
        
        try:
            # Import the appropriate analyzer class
            if analyzer_type == "swing":
                # Only pass the parameters that SwingDetector expects
                lookback = analyzer_config.get('lookback', 5)  # default to 5 if not specified
                return SwingDetector(lookback=lookback)
            
            elif analyzer_type == "trend":
                # Only pass the parameters that TrendAnalyzer expects
                lookback = analyzer_config.get('lookback', 2)  # default to 2 if not specified
                return TrendAnalyzer(lookback=lookback)
            
            elif analyzer_type == "range":
                # Only pass the parameters that RangeDetector expects
                min_touches = analyzer_config.get('min_touches', 3)
                min_range_size = analyzer_config.get('min_range_size', 0.5)
                max_lookback = analyzer_config.get('max_lookback', 100)
                return RangeDetector(
                    min_touches=min_touches,
                    min_range_size=min_range_size,
                    max_lookback=max_lookback
                )
            
            elif analyzer_type == "fibbonacci":
                # Only pass the parameters that FibonacciAnalyzer expects
                buffer_percent = analyzer_config.get('buffer_percent', 0.5)
                return FibonacciAnalyzer(buffer_percent=buffer_percent)
            
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
