
import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from scripts.utils.financial_utils import convert_ytd_to_ttm

class OCFtoNI(BaseFactor):
    """
    Operating Cashflow / Net Income (TTM).
    """
    
    @property
    def name(self) -> str:
        return "OCFtoNI"
        
    @property
    def required_fields(self) -> list:
        return ['n_cashflow_act', 'n_income']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate OCF / Net Income (TTM).
        Assumes df is sorted by [ts_code, end_date].
        """
        self.check_dependencies(df)
        
        # Calculate TTM for Cashflow and Net Income (Flow variables)
        # Already converted to TTM in construct_fundamental_factors.py
        
        factor_value = df['n_cashflow_act'] / df['n_income']
        
        # Handle division by zero or infinites
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        # If ann_date exists, preserve it
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
