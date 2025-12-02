
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class RelativeVigorIndex(BaseFactor):
    """
    Relative Vigor Index (RVI) Factor.
    Measures the conviction of a recent price action (Close-Open vs High-Low).
    RVI = SMA(Close-Open, N) / SMA(High-Low, N)
    """
    
    @property
    def name(self) -> str:
        return "RVI"
        
    @property
    def required_fields(self) -> list:
        return ['open', 'high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RVI.
        
        Args:
            df: Daily dataframe with 'open', 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'RVI' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 10
        
        numerator = df['close'] - df['open']
        denominator = df['high'] - df['low']
        
        # Rolling sums (or means, ratio is same)
        rolling_num = numerator.groupby(df['ts_code']).rolling(window).mean().reset_index(0, drop=True)
        rolling_den = denominator.groupby(df['ts_code']).rolling(window).mean().reset_index(0, drop=True)
        
        # RVI
        rvi = rolling_num / rolling_den.replace(0, np.nan)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: rvi,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
