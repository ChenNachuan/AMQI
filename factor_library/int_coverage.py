
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
        # Already converted to TTM in construct_fundamental_factors.py
        
        # Handle missing interest expense (assume 0 if missing)
        int_exp = df['int_exp'].fillna(0)
        
        # Avoid division by zero
        # If int_exp is 0:
        # - If OCF > 0, coverage is infinite (set to a high cap, e.g., 100)
        # - If OCF <= 0, coverage is bad (set to a low value, e.g., -100 or keep as is)
        # For simplicity and robustness, we can add a small epsilon or use conditional logic.
        # Here we use a safe division approach:
        
        # Create a mask for zero interest expense
        zero_int_mask = (int_exp == 0)
        
        # Calculate ratio where int_exp != 0
        factor_value = df['n_cashflow_act'] / int_exp
        
        # Handle zero interest expense cases
        # If int_exp is 0 and OCF >= 0, it's very good (infinite coverage). Cap at 100.
        factor_value.loc[zero_int_mask & (df['n_cashflow_act'] >= 0)] = 100
        # If int_exp is 0 and OCF < 0, it's bad (negative coverage). Cap at -100.
        factor_value.loc[zero_int_mask & (df['n_cashflow_act'] < 0)] = -100
        
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
