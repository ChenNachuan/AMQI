import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class VolumeConfirmation(BaseFactor):
    """
    Volume Confirmation Factor.
    Identifies stocks with expanding volume (increased market attention).
    
    Formula:
    1. vol_ma = MA(vol, period)
    2. vol_ratio = vol / vol_ma
    
    A value > 1.2 typically indicates significant volume expansion.
    """
    
    @property
    def name(self) -> str:
        return "Volume_Confirmation"
        
    @property
    def required_fields(self) -> list:
        return ['vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Volume Confirmation.
        
        Args:
            df: Daily dataframe with 'vol'.
            
        Returns:
            DataFrame with 'Volume_Confirmation' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        period = 14
        
        # Calculate volume moving average
        vol_ma = df.groupby('ts_code')['vol'].rolling(period).mean().reset_index(0, drop=True)
        
        # Calculate volume ratio
        vol_ratio = df['vol'] / vol_ma
        
        # Prepare result
        result = pd.DataFrame({
            self.name: vol_ratio,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
