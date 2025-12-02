
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class FARatio(BaseFactor):
    """
    Fixed Asset Ratio.
    Fixed Assets / Total Assets.
    """
    
    @property
    def name(self) -> str:
        return "FARatio"
        
    @property
    def required_fields(self) -> list:
        return ['fix_assets', 'total_assets']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        factor_value = df['fix_assets'] / df['total_assets']
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
