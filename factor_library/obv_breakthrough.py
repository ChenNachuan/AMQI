import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class OBVBreakthrough(BaseFactor):
    """
    OBV Breakthrough Factor.
    Measures the strength of OBV breaking through historical highs.
    Identifies accelerating capital inflow signals.
    """
    
    @property
    def name(self) -> str:
        return "OBV_Breakthrough"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame, rank_period: int = 120) -> pd.DataFrame:
        """
        Calculate OBV breakthrough factor.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            rank_period: Period for breakthrough calculation, default 120 days.
            
        Returns:
            DataFrame with 'OBV_Breakthrough' column (clipped to [-5, 5]).
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
        
        # Calculate historical high OBV
        obv_high = obv.groupby(df['ts_code']).rolling(rank_period).max().reset_index(level=0, drop=True)
        
        # Previous day's historical high
        obv_high_shifted = obv_high.groupby(df['ts_code']).shift(1)
        
        # Safe breakthrough strength calculation
        obv_breakthrough = np.where(
            obv_high_shifted.abs() > 1e-10,
            (obv - obv_high_shifted) / obv_high_shifted.abs(),
            0
        )
        
        # Clip to [-5, 5] range (±500%)
        obv_breakthrough = pd.Series(obv_breakthrough, index=obv.index).clip(-5, 5)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: obv_breakthrough,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
