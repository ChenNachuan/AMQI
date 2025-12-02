
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class EquityRatio(BaseFactor):
    """
    Equity Ratio.
    Total Equity / Total Assets.
    """
    
    @property
    def name(self) -> str:
        return "EquityRatio"
        
    @property
    def required_fields(self) -> list:
        return ['total_hldr_eqy_inc_min_int', 'total_assets']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        factor_value = df['total_hldr_eqy_inc_min_int'] / df['total_assets']
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
