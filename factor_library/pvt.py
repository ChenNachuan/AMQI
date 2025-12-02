
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class PriceVolumeTrend(BaseFactor):
    """
    Price Volume Trend (PVT) Factor.
    Similar to OBV but using percentage price change.
    PVT = Cumulative Sum of (Volume * (Close - PrevClose) / PrevClose)
    Returns: 10-day change of PVT.
    """
    
    @property
    def name(self) -> str:
        return "PVT"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate PVT and return its 10-day change.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            
        Returns:
            DataFrame with 'PVT' column (stationary).
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Calculate PVT
        prev_close = df.groupby('ts_code')['close'].shift(1)
        
        # Percentage change
        pct_change = (df['close'] - prev_close) / prev_close
        
        # PVT increment
        pvt_inc = pct_change * df['vol']
        
        # Cumulative Sum
        pvt = pvt_inc.groupby(df['ts_code']).cumsum()
        
        # Refinement: 10-day change
        pvt_change = pvt.groupby(df['ts_code']).diff(10)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: pvt_change,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
