import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class ATRTrend(BaseFactor):
    """
    ATR Trend Factor.
    Identifies stocks where ATR is in an upward trend (sustained volatility expansion).
    
    Formula:
    1. ATR_norm = ATR / MA(close, period)
    2. ATR_short_ma = MA(ATR_norm, 5)
    3. ATR_long_ma = MA(ATR_norm, 20)
    4. ATR_trend = 1 if ATR_short_ma > ATR_long_ma else 0
    
    Returns 1 for upward trend, 0 for downward trend.
    """
    
    @property
    def name(self) -> str:
        return "ATR_Trend"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate ATR Trend.
        
        Args:
            df: Daily dataframe with 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'ATR_Trend' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        period = 14
        
        # Calculate True Range
        prev_close = df.groupby('ts_code')['close'].shift(1)
        high = df['high']
        low = df['low']
        
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Calculate ATR
        atr = tr.groupby(df['ts_code']).rolling(period).mean().reset_index(0, drop=True)
        
        # Calculate price mean for normalization
        price_mean = df.groupby('ts_code')['close'].rolling(period).mean().reset_index(0, drop=True)
        
        # Normalized ATR
        atr_norm = atr / price_mean
        
        # Calculate short and long term moving averages
        atr_short_ma = atr_norm.groupby(df['ts_code']).rolling(5).mean().reset_index(0, drop=True)
        atr_long_ma = atr_norm.groupby(df['ts_code']).rolling(20).mean().reset_index(0, drop=True)
        
        # Determine trend (1 for upward, 0 for downward)
        atr_trend = (atr_short_ma > atr_long_ma).astype(int)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: atr_trend,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
