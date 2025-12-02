
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class AverageTrueRange(BaseFactor):
    """
    Average True Range (ATR) Factor.
    TR = max(High-Low, |High-Close_prev|, |Low-Close_prev|)
    ATR = Rolling mean of TR
    """
    
    @property
    def name(self) -> str:
        return "ATR"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate ATR.
        
        Args:
            df: Daily dataframe with 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'ATR' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Calculate True Range
        # We need previous close
        prev_close = df.groupby('ts_code')['close'].shift(1)
        
        high = df['high']
        low = df['low']
        
        # TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Calculate ATR (e.g., 14-day rolling mean)
        window = 14
        atr = tr.groupby(df['ts_code']).rolling(window).mean().reset_index(0, drop=True)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: atr,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
