
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class Reversal(BaseFactor):
    """
    Short-term Reversal Factor (Srev).
    Lagged monthly return (or 20-day return).
    """
    
    @property
    def name(self) -> str:
        return "Srev"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Reversal.
        Logic: Return of the previous month (t-20 to t).
        Or simply lagged return?
        Usually Srev is the return of month t-1.
        So R_{t-1}.
        
        Args:
            df: DataFrame with 'close'.
            
        Returns:
            DataFrame with 'Srev' column.
        """
        self.check_dependencies(df)
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 20 # 1 month
        
        # Calculate 1-month return
        # P_t / P_{t-20} - 1
        # But Reversal is usually the PAST month's return, used to predict NEXT month.
        # If we are at time t, Srev is Ret_{t-20, t}.
        
        p_t = df.groupby('ts_code')['close'].shift(0) # Current
        p_t_minus_20 = df.groupby('ts_code')['close'].shift(window)
        
        srev = (p_t / p_t_minus_20) - 1
        
        # Result
        result = pd.DataFrame({
            self.name: srev,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
