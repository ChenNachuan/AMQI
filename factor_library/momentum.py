
import pandas as pd
from .base_factor import BaseFactor

class MomentumFactor(BaseFactor):
    def calculate(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculates the 12-2 month momentum factor.
        Assumes 'r11' is already in the dataset, but this demonstrates the interface.
        If 'r11' wasn't there, we would calculate it from 'ret'.
        """
        # Ensure index is set for alignment if not already
        if 'stkcd' in df.columns:
            df = df.set_index(['stkcd', 'year', 'month'])
            
        # Example: Just returning the existing 'r11' column as the factor
        # In a real scenario, you might calculate: df['ret'].groupby('stkcd').rolling(11).sum().shift(1) etc.
        if 'r11' in df.columns:
            return df['r11']
        else:
            raise ValueError("Column 'r11' not found in dataset.")
