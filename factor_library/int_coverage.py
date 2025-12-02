
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class IntCoverage(BaseFactor):
    """
    Interest Coverage.
    OCF (TTM) / Interest Expense (TTM).
    """
    
    @property
    def name(self) -> str:
        return "IntCoverage"
        
    @property
    def required_fields(self) -> list:
        return ['n_cashflow_act', 'int_exp']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        # TTM OCF
        ocf_ttm = df.groupby('ts_code')['n_cashflow_act'].rolling(4).sum().reset_index(level=0, drop=True)
        
        # TTM Interest Expense
        int_ttm = df.groupby('ts_code')['int_exp'].rolling(4).sum().reset_index(level=0, drop=True)
        
        factor_value = ocf_ttm / int_ttm
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
