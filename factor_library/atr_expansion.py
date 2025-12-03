import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class ATRExpansion(BaseFactor):
    """
    ATR Expansion Factor.
    Identifies stocks where ATR is rapidly rising from low levels (volatility expansion).
    
    Formula:
    1. ATR_norm = ATR / MA(close, period)
    2. ATR_ma = MA(ATR_norm, period)
    3. ATR_expansion = (ATR_norm / ATR_ma - 1) × 100
    """
    
    @property
    def name(self) -> str:
        return "ATR_Expansion"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate ATR Expansion.
        
        Args:
            df: Daily dataframe with 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'ATR_Expansion' column.
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
        
        # ATR moving average
        atr_ma = atr_norm.groupby(df['ts_code']).rolling(period).mean().reset_index(0, drop=True)
        
        # ATR expansion ratio (in percentage)
        atr_expansion = (atr_norm / atr_ma - 1) * 100
        
        # Prepare result
        result = pd.DataFrame({
            self.name: atr_expansion,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
