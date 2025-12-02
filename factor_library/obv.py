
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class OnBalanceVolume(BaseFactor):
    """
    On Balance Volume (OBV) Factor.
    Cumulative volume with direction determined by price change.
    Returns: 10-day slope or pct_change of OBV to make it stationary.
    """
    
    @property
    def name(self) -> str:
        return "OBV"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate OBV and return its 10-day change.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            
        Returns:
            DataFrame with 'OBV' column (stationary).
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Calculate OBV
        # If Close > PrevClose, Vol is positive.
        # If Close < PrevClose, Vol is negative.
        # If Close == PrevClose, Vol is 0.
        
        prev_close = df.groupby('ts_code')['close'].shift(1)
        
        vol_direction = pd.Series(0, index=df.index)
        vol_direction[df['close'] > prev_close] = 1
        vol_direction[df['close'] < prev_close] = -1
        
        signed_vol = vol_direction * df['vol']
        
        # Cumulative Sum
        obv = signed_vol.groupby(df['ts_code']).cumsum()
        
        # Refinement: 10-day slope or pct_change
        # Since OBV can be negative or zero, pct_change might be unstable.
        # Slope (linear regression) or simple difference?
        # User said: "10-day slope or pct_change".
        # Let's use 10-day difference (change) normalized by something?
        # Or just 10-day difference.
        # "pct_change" on cumulative series that crosses zero is bad.
        # Let's use 10-day linear slope (trend) or just 10-day change.
        # Let's use 10-day change (OBV_t - OBV_{t-10}).
        # To make it more comparable across stocks, maybe divide by average volume?
        # User didn't specify normalization, but "pct_change" implies some normalization.
        # Let's try (OBV_t - OBV_{t-10}) / Average(Abs(OBV))? No.
        # Let's stick to simple 10-day change or slope.
        # Let's implement 10-day change.
        
        obv_change = obv.groupby(df['ts_code']).diff(10)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: obv_change,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
