
import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from scripts.utils.financial_utils import convert_ytd_to_ttm

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
        
        # Calculate TTM for OCF and Interest Expense (Flow variables)
        df = convert_ytd_to_ttm(df, 'n_cashflow_act')
        df = convert_ytd_to_ttm(df, 'int_exp')
        
        factor_value = df['n_cashflow_act_ttm'] / df['int_exp_ttm']
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
