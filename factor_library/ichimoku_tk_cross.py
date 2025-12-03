import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class IchimokuTKCross(BaseFactor):
    """
    Ichimoku TK Cross Factor.
    
    Calculates the cross signal between Tenkan-sen and Kijun-sen:
    - 1: Golden cross (Tenkan > Kijun, bullish signal)
    - -1: Death cross (Tenkan <= Kijun, bearish signal)
    
    Golden cross indicates buy signal, death cross indicates sell signal.
    """
    
    @property
    def name(self) -> str:
        return "IchimokuTKCross"
        
    @property
    def required_fields(self) -> list:
        return ['high', 'low']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Ichimoku TK Cross factor.
        
        Args:
            df: Daily dataframe with 'high', 'low'.
            
        Returns:
            DataFrame with 'IchimokuTKCross' column.
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
        
        # Calculate TK cross score
        factor_value = np.where(tenkan_sen > kijun_sen, 1, -1)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: factor_value,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
