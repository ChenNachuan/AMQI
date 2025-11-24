
import pandas as pd

class LinearModel:
    def __init__(self, weights: dict = None):
        """
        Args:
            weights: Dictionary of factor names and their weights.
                     If None, assumes equal weights.
        """
        self.weights = weights

    def combine(self, factors: pd.DataFrame) -> pd.Series:
        """
        Combines multiple factors into a single signal.

        Args:
            factors: DataFrame where columns are factor names and index is ['stkcd', 'year', 'month'].

        Returns:
            pd.Series: Combined signal.
        """
        if self.weights is None:
            # Equal weight
            return factors.mean(axis=1)
        
        # Weighted sum
        weighted_sum = pd.Series(0, index=factors.index)
        for factor_name, weight in self.weights.items():
            if factor_name in factors.columns:
                weighted_sum += factors[factor_name] * weight
        
        return weighted_sum
