
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class TripleEMA(BaseFactor):
    """
    Triple Exponential Moving Average (TEMA) Factor.
    TEMA = 3*EMA1 - 3*EMA2 + EMA3
    Returns: Close - TEMA (Trend Deviation)
    """
    
    @property
    def name(self) -> str:
        return "TEMA"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate TEMA factor (Close - TEMA).
        
        Args:
            df: Daily dataframe with 'close'.
            
        Returns:
            DataFrame with 'TEMA' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 30 # Standard default, or maybe shorter like 10? TEMA is fast. Let's use 20.
        
        # EMA1
        ema1 = df.groupby('ts_code')['close'].ewm(span=window, adjust=False).mean().reset_index(0, drop=True)
        
        # EMA2
        ema2 = ema1.groupby(df['ts_code']).ewm(span=window, adjust=False).mean().reset_index(0, drop=True)
        
        # EMA3
        ema3 = ema2.groupby(df['ts_code']).ewm(span=window, adjust=False).mean().reset_index(0, drop=True)
        
        # TEMA
        tema = 3 * ema1 - 3 * ema2 + ema3
        
        # Factor: Close - TEMA
        factor_value = df['close'] - tema
        
        # Prepare result
        result = pd.DataFrame({
            self.name: factor_value,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
