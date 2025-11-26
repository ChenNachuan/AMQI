
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class Turnover(BaseFactor):
    """
    Turnover Factor (TUR).
    Monthly average daily turnover rate.
    """
    
    @property
    def name(self) -> str:
        return "TUR"
        
    @property
    def required_fields(self) -> list:
        return ['turnover_rate']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Turnover.
        
        Args:
            df: DataFrame with 'turnover_rate'.
            
        Returns:
            DataFrame with 'TUR' column.
        """
        self.check_dependencies(df)
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 20 # Approx 1 month
        
        # Rolling mean
        tur = df.groupby('ts_code')['turnover_rate'].rolling(window).mean().reset_index(0, drop=True)
        
        # Result
        result = pd.DataFrame({
            self.name: tur,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
