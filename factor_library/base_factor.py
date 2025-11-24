
import pandas as pd
from abc import ABC, abstractmethod

class BaseFactor(ABC):
    """
    Abstract base class for all factors.
    """
    
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculates the factor values.

        Args:
            df (pd.DataFrame): The full dataset containing stock data. 
                               Must contain 'stkcd', 'year', 'month' columns.

        Returns:
            pd.Series: A Series of factor values, indexed by ['stkcd', 'year', 'month'].
        """
        pass

def check_factor_format(factor_values: pd.Series) -> bool:
    """
    Checks if the factor output format is correct.
    """
    if not isinstance(factor_values, pd.Series):
        print("Error: Factor must return a pandas Series.")
        return False
    
    if not isinstance(factor_values.index, pd.MultiIndex):
        print("Error: Factor index must be a MultiIndex.")
        return False
        
    if factor_values.index.names != ['stkcd', 'year', 'month']:
        # Try to handle case where names are None but levels match
        if len(factor_values.index.levels) == 3:
             factor_values.index.names = ['stkcd', 'year', 'month']
             return True
        print(f"Error: Index names must be ['stkcd', 'year', 'month'], got {factor_values.index.names}")
        return False
        
    return True
