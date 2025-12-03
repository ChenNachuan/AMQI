import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class OBVDivergence(BaseFactor):
    """
    Price-Volume Divergence Factor.
    Measures the difference between OBV trend and price trend.
    Identifies potential trend reversals through divergence signals.
    """
    
    @property
    def name(self) -> str:
        return "OBV_Divergence"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame, divergence_period: int = 20) -> pd.DataFrame:
        """
        Calculate price-volume divergence factor.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            divergence_period: Period for divergence calculation, default 20 days.
            
        Returns:
            DataFrame with 'OBV_Divergence' column.
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
        
        # Calculate slope function
        def calc_slope(series):
            """Calculate linear regression slope"""
            if len(series) < divergence_period or series.isna().any():
                return np.nan
            x = np.arange(len(series))
            y = series.values
            try:
                slope = np.polyfit(x, y, 1)[0]
                return slope
            except:
                return np.nan
        
        # Calculate price trend slope
        price_slope = df.groupby('ts_code')['close'].rolling(divergence_period).apply(calc_slope, raw=False).reset_index(level=0, drop=True)
        
        # Calculate OBV trend slope
        obv_slope = obv.groupby(df['ts_code']).rolling(divergence_period).apply(calc_slope, raw=False).reset_index(level=0, drop=True)
        
        # Safe normalization function
        def safe_normalize(series, group_key):
            """Safe normalization to avoid division by zero"""
            def normalize_group(group):
                mean = group.mean()
                std = group.std()
                if pd.isna(mean) or pd.isna(std) or std < 1e-8:
                    return pd.Series(0, index=group.index)
                return (group - mean) / std
            
            return series.groupby(group_key).transform(normalize_group)
        
        # Normalize and calculate divergence
        price_slope_norm = safe_normalize(price_slope, df['ts_code'])
        obv_slope_norm = safe_normalize(obv_slope, df['ts_code'])
        obv_divergence = obv_slope_norm - price_slope_norm
        
        # Prepare result
        result = pd.DataFrame({
            self.name: obv_divergence,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
