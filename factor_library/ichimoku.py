
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class Ichimoku(BaseFactor):
    """
    Ichimoku Kinko Hyo Factor.
    Returns: Close - Kijun_sen (Base Line)
    Kijun_sen = (26-period High + 26-period Low) / 2
    """
    
    @property
    def name(self) -> str:
        return "Ichimoku"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Ichimoku factor (Close - Kijun_sen).
        
        Args:
            df: Daily dataframe with 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'Ichimoku' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 26
        
        # Calculate Kijun-sen (Base Line)
        # (Highest High + Lowest Low) / 2 over past 26 periods
        
        rolling = df.groupby('ts_code')[['high', 'low']].rolling(window)
        period_high = rolling['high'].max().reset_index(0, drop=True)
        period_low = rolling['low'].min().reset_index(0, drop=True)
        
        kijun_sen = (period_high + period_low) / 2
        
        # Factor: Close - Kijun_sen
        factor_value = df['close'] - kijun_sen
        
        # Prepare result
        result = pd.DataFrame({
            self.name: factor_value,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
