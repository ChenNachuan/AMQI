import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class PricePosition(BaseFactor):
    """
    Price Position Factor.
    Identifies stocks where price is near recent highs (upward trend).
    
    Formula:
    1. price_high = MAX(high, period×2)
    2. price_low = MIN(low, period×2)
    3. price_position = (close - price_low) / (price_high - price_low)
    
    Value ranges from 0 to 1, where 1 means price is at the highest point in the window.
    """
    
    @property
    def name(self) -> str:
        return "Price_Position"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Price Position.
        
        Args:
            df: Daily dataframe with 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'Price_Position' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        period = 14
        window = period * 2
        
        # Calculate recent highest and lowest prices
        price_high = df.groupby('ts_code')['high'].rolling(window).max().reset_index(0, drop=True)
        price_low = df.groupby('ts_code')['low'].rolling(window).min().reset_index(0, drop=True)
        
        # Calculate price position
        price_position = (df['close'] - price_low) / (price_high - price_low)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: price_position,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
