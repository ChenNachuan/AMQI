
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class Momentum(BaseFactor):
    """
    Momentum Factor (R11).
    Cumulative return from t-12 to t-1 (skipping most recent month).
    Calculated on a daily rolling basis.
    """
    
    @property
    def name(self) -> str:
        return "R11"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate R11 momentum on daily rolling basis.
        
        Logic:
        R11 = P_{t-21} / P_{t-252} - 1
        (Approximating 1 month as 21 trading days, 12 months as 252 trading days)
           
        Args:
            df: Daily dataframe with 'close'.
            
        Returns:
            DataFrame with 'R11' column, indexed by [trade_date, ts_code].
            Returns daily values.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Define window sizes (approximate trading days)
        lag_1m = 21
        lag_12m = 252
        
        # Calculate R11
        # P_{t-lag_1m} / P_{t-lag_12m} - 1
        # Group by ts_code to avoid cross-stock shifting
        
        p_t_minus_1m = df.groupby('ts_code')['close'].shift(lag_1m)
        p_t_minus_12m = df.groupby('ts_code')['close'].shift(lag_12m)
        
        r11 = (p_t_minus_1m / p_t_minus_12m) - 1
        
        # Prepare result
        result = pd.DataFrame({
            self.name: r11,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        # Set index to match standard [trade_date, ts_code]
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
