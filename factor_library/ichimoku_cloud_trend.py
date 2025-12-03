import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class IchimokuCloudTrend(BaseFactor):
    """
    Ichimoku Cloud Trend Factor.
    
    Calculates the trend direction of the Ichimoku cloud:
    - 1: Bullish cloud (Senkou Span A > Senkou Span B)
    - 0: Bearish cloud (Senkou Span A <= Senkou Span B)
    
    Bullish cloud indicates uptrend, bearish cloud indicates downtrend.
    """
    
    @property
    def name(self) -> str:
        return "IchimokuCloudTrend"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Ichimoku Cloud Trend factor.
        
        Args:
            df: Daily dataframe with 'high', 'low'.
            
        Returns:
            DataFrame with 'IchimokuCloudTrend' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # Calculate Tenkan-sen (Conversion Line): 9-period
        rolling_9 = df.groupby('ts_code')[['high', 'low']].rolling(9)
        tenkan_high = rolling_9['high'].max().reset_index(0, drop=True)
        tenkan_low = rolling_9['low'].min().reset_index(0, drop=True)
        tenkan_sen = (tenkan_high + tenkan_low) / 2
        
        # Calculate Kijun-sen (Base Line): 26-period
        rolling_26 = df.groupby('ts_code')[['high', 'low']].rolling(26)
        kijun_high = rolling_26['high'].max().reset_index(0, drop=True)
        kijun_low = rolling_26['low'].min().reset_index(0, drop=True)
        kijun_sen = (kijun_high + kijun_low) / 2
        
        # Calculate Senkou Span A: (Tenkan + Kijun) / 2, shifted forward 26 periods
        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)
        
        # Calculate Senkou Span B: 52-period, shifted forward 26 periods
        rolling_52 = df.groupby('ts_code')[['high', 'low']].rolling(52)
        senkou_b_high = rolling_52['high'].max().reset_index(0, drop=True)
        senkou_b_low = rolling_52['low'].min().reset_index(0, drop=True)
        senkou_span_b = ((senkou_b_high + senkou_b_low) / 2).shift(26)
        
        # Calculate cloud trend score
        factor_value = (senkou_span_a > senkou_span_b).astype(int)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: factor_value,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
