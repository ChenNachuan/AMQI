
import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from scripts.utils.financial_utils import convert_ytd_to_ttm

class FATurnover(BaseFactor):
    """
    Fixed Asset Turnover.
    Revenue (TTM) / Average Fixed Assets.
    """
    
    @property
    def name(self) -> str:
        return "FATurnover"
        
    @property
    def required_fields(self) -> list:
        return ['revenue', 'fix_assets']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        # Calculate TTM for Revenue (Flow variable)
        # Already converted to TTM in construct_fundamental_factors.py
        
        # Average Fixed Assets (Stock variable)
        avg_fa = df.groupby('ts_code')['fix_assets'].rolling(4).mean().reset_index(level=0, drop=True)
        
        factor_value = df['revenue'] / avg_fa
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
