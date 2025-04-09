import logging
from typing import Dict, List, Any, Set, Optional

logger = logging.getLogger(__name__)

class IndicatorDAG:
    """
    Manages indicator dependencies and execution order using a Directed Acyclic Graph.
    Ensures indicators are executed in the correct order based on their dependencies.
    """
    
    def __init__(self):
        """Initialize the indicator DAG."""
        self.indicators = {}
        self.dependencies = {}
        self.execution_order = []
    
    def register_indicator(self, name: str, indicator_instance: Any, dependencies: Optional[List[str]] = None):
        """
        Register an indicator with its dependencies.
        
        Args:
            name: Name of the indicator
            indicator_instance: The indicator instance
            dependencies: List of indicator names this indicator depends on
        """
        self.indicators[name] = indicator_instance
        self.dependencies[name] = dependencies or []
        # Reset execution order when registration changes
        self.execution_order = []
        logger.debug(f"Registered indicator '{name}' with dependencies: {dependencies}")
    
    def compute_execution_order(self) -> List[str]:
        """
        Compute the optimal execution order using topological sort.
        
        Returns:
            List of indicator names in execution order
        """
        if self.execution_order:
            return self.execution_order
            
        # Initialize variables for topological sort
        visited = set()
        temp_mark = set()
        order = []
        
        def visit(node):
            if node in temp_mark:
                raise ValueError(f"Circular dependency detected involving {node}")
            if node not in visited:
                temp_mark.add(node)
                for dep in self.dependencies.get(node, []):
                    if dep not in self.indicators:
                        logger.warning(f"Dependency '{dep}' for indicator '{node}' not found")
                        continue
                    visit(dep)
                temp_mark.remove(node)
                visited.add(node)
                order.append(node)
        
        # Visit all nodes
        for node in self.indicators:
            if node not in visited:
                visit(node)
        
        # Reverse for correct execution order
        self.execution_order = list(order)
        logger.info(f"Computed indicator execution order: {self.execution_order}")
        return self.execution_order
    
    async def run_indicators(self, data: Dict[str, Any], requested_indicators: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run indicators in optimal order.
        
        Args:
            data: Input market data
            requested_indicators: Specific indicators to run (if None, runs all)
            
        Returns:
            Dictionary of indicator results
        """
        # Compute execution order if not already done
        execution_order = self.compute_execution_order()
        
        # Filter to requested indicators if specified
        if requested_indicators is not None:
            # Also include dependencies
            required = set(requested_indicators)
            for indicator in requested_indicators:
                # Add all dependencies recursively
                deps_to_process = list(self.dependencies.get(indicator, []))
                while deps_to_process:
                    dep = deps_to_process.pop(0)
                    required.add(dep)
                    deps_to_process.extend([d for d in self.dependencies.get(dep, []) if d not in required])
            
            # Filter execution order to required indicators
            execution_order = [ind for ind in execution_order if ind in required]
        
        # Run indicators in order
        results = {}
        for indicator_name in execution_order:
            try:
                # Prepare enhanced data with dependency results
                enhanced_data = data.copy()
                for dep_name in self.dependencies.get(indicator_name, []):
                    if dep_name in results:
                        enhanced_data[f"{dep_name}_data"] = results[dep_name]
                
                # Run the indicator
                indicator = self.indicators[indicator_name]
                results[indicator_name] = await indicator.calculate(enhanced_data)
                logger.debug(f"Executed indicator '{indicator_name}'")
            except Exception as e:
                logger.error(f"Error executing indicator '{indicator_name}': {e}", exc_info=True)
                # Continue with other indicators despite errors
                results[indicator_name] = {"error": str(e)}
        
        return results