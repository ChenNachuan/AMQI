import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class PriceBreakout(BaseFactor):
    """
    Price Breakout Factor.
    Identifies stocks breaking above recent highs (momentum signal).
    
    Formula:
    1. prev_high = MAX(high, period).shift(1)
    2. price_breakout = 1 if close > prev_high else 0
    
    Returns 1 when price breaks out, 0 otherwise.
    """
    
    @property
    def name(self) -> str:
        return "Price_Breakout"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Price Breakout.
        
        Args:
            df: Daily dataframe with 'high', 'close'.
            
        Returns:
            DataFrame with 'Price_Breakout' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        period = 14
        
        # Calculate previous period's highest price (excluding current day)
        prev_high = df.groupby('ts_code')['high'].rolling(period).max().reset_index(0, drop=True).groupby(df['ts_code']).shift(1)
        
        # Determine if price breaks out
        price_breakout = (df['close'] > prev_high).astype(int)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: price_breakout,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
