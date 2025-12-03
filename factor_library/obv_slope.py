import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class OBVSlope(BaseFactor):
    """
    OBV Trend Slope Factor.
    Measures the speed and direction of capital flow using linear regression slope.
    """
    
    @property
    def name(self) -> str:
        return "OBV_Slope"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame, trend_period: int = 20) -> pd.DataFrame:
        """
        Calculate OBV trend slope factor.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            trend_period: Period for slope calculation, default 20 days.
            
        Returns:
            DataFrame with 'OBV_Slope' column.
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
        
        # Calculate slope using linear regression
        def calc_slope(series):
            """Calculate linear regression slope"""
            if len(series) < trend_period or series.isna().any():
                return np.nan
            x = np.arange(len(series))
            y = series.values
            try:
                slope = np.polyfit(x, y, 1)[0]
                return slope
            except:
                return np.nan
        
        obv_slope = obv.groupby(df['ts_code']).rolling(trend_period).apply(calc_slope, raw=False).reset_index(level=0, drop=True)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: obv_slope,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
