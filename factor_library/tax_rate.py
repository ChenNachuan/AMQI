
import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from scripts.utils.financial_utils import convert_ytd_to_ttm

class TaxRate(BaseFactor):
    """
    Effective Tax Rate.
    Income Tax (TTM) / Total Profit (TTM).
    """
    
    @property
    def name(self) -> str:
        return "TaxRate"
        
    @property
    def required_fields(self) -> list:
        return ['income_tax', 'total_profit']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        # Calculate TTM for Income Tax and Total Profit (Flow variables)
        # Already converted to TTM in construct_fundamental_factors.py
        
        factor_value = df['income_tax'] / df['total_profit']
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
