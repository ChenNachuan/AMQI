import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class OBVChangeRate(BaseFactor):
    """
    OBV Change Rate Factor.
    Measures the growth or decay rate of cumulative volume energy.
    """
    
    @property
    def name(self) -> str:
        return "OBV_Change_Rate"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame, trend_period: int = 20) -> pd.DataFrame:
        """
        Calculate OBV change rate factor.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            trend_period: Period for change rate calculation, default 20 days.
            
        Returns:
            DataFrame with 'OBV_Change_Rate' column (clipped to [-10, 10]).
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Calculate OBV first
        prev_close = df.groupby('ts_code')['close'].shift(1)
        
        vol_direction = pd.Series(0, index=df.index)
        vol_direction[df['close'] > prev_close] = 1
        vol_direction[df['close'] < prev_close] = -1
        
        signed_vol = vol_direction * df['vol']
        obv = signed_vol.groupby(df['ts_code']).cumsum()
        
        # Calculate OBV value from trend_period days ago
        obv_shifted = obv.groupby(df['ts_code']).shift(trend_period)
        
        # Safe percentage change calculation to avoid division by zero
        obv_change = np.where(
            obv_shifted.abs() > 1e-10,
            (obv - obv_shifted) / obv_shifted.abs(),
            0
        )
        
        # Clip to [-10, 10] range (±1000%)
        obv_change = pd.Series(obv_change, index=obv.index).clip(-10, 10)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: obv_change,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
