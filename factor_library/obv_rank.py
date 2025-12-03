import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class OBVRank(BaseFactor):
    """
    OBV Relative Strength Factor.
    Measures the percentile rank of current OBV within historical window.
    """
    
    @property
    def name(self) -> str:
        return "OBV_Rank"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'vol']
        
    def calculate(self, df: pd.DataFrame, rank_period: int = 120) -> pd.DataFrame:
        """
        Calculate OBV relative strength factor.
        
        Args:
            df: Daily dataframe with 'close', 'vol'.
            rank_period: Period for percentile ranking, default 120 days.
            
        Returns:
            DataFrame with 'OBV_Rank' column (range [0, 1]).
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
        
        # Calculate percentile rank
        def calc_rank(series):
            if len(series) < rank_period:
                return np.nan
            return pd.Series(series).rank(pct=True).iloc[-1]
        
        obv_rank = obv.groupby(df['ts_code']).rolling(rank_period).apply(calc_rank, raw=False).reset_index(level=0, drop=True)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: obv_rank,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
