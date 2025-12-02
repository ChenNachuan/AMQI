
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class BollingerBands(BaseFactor):
    """
    Bollinger Bands Factor.
    Returns Percent B: (Close - LowerBand) / (UpperBand - LowerBand)
    """
    
    @property
    def name(self) -> str:
        return "Bollinger"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Bollinger Bands Percent B.
        
        Args:
            df: Daily dataframe with 'close'.
            
        Returns:
            DataFrame with 'Bollinger' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 20
        num_std = 2
        
        # Rolling mean and std
        rolling = df.groupby('ts_code')['close'].rolling(window)
        ma = rolling.mean().reset_index(0, drop=True)
        std = rolling.std().reset_index(0, drop=True)
        
        upper = ma + num_std * std
        lower = ma - num_std * std
        
        # Percent B
        # Handle division by zero if upper == lower (though unlikely for price data)
        denominator = upper - lower
        percent_b = (df['close'] - lower) / denominator
        
        # Prepare result
        result = pd.DataFrame({
            self.name: percent_b,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
