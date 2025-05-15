from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class StrengthDto:
    """Data transfer object for order block strength calculation results."""
    
    # Overall strength score (0-1)
    overall_score: float
    
    # Individual component scores
    swing_proximity: float
    fib_confluence: float
    mtf_confluence: float
    
    # Weights used for each component
    weights: Dict[str, float]
    
    # Optional raw data for additional analysis
    raw_data: Dict[str, Any] = None
    
    def __post_init__(self):
        """Validate that the overall score is correctly calculated from components."""
        # Optional: Add validation that overall_score matches weighted sum of components
        calculated_score = (
            self.weights.get('swing_proximity', 0) * self.swing_proximity +
            self.weights.get('fib_confluence', 0) * self.fib_confluence + 
            self.weights.get('mtf_confluence', 0) * self.mtf_confluence
        )
        
        # Allow for small floating point differences
        if abs(calculated_score - self.overall_score) > 0.0001:
            raise ValueError(
                f"Overall score ({self.overall_score}) doesn't match "
                f"calculated weighted sum ({calculated_score})"
            )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'overall_score': self.overall_score,
            'swing_proximity': self.swing_proximity,
            'fib_confluence': self.fib_confluence,
            'mtf_confluence': self.mtf_confluence,
            'weights': self.weights,
            'raw_data': self.raw_data
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StrengthDto':
        """Create a StrengthDto from a dictionary."""
        return cls(
            overall_score=data.get('overall_score', 0.0),
            swing_proximity=data.get('swing_proximity', 0.0),
            fib_confluence=data.get('fib_confluence', 0.0),
            mtf_confluence=data.get('mtf_confluence', 0.0),
            weights=data.get('weights', {}),
            raw_data=data.get('raw_data')
        )