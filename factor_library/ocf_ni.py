
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

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
        
        # TTM Calculation: Rolling sum of last 4 quarters
        # Note: This assumes data is quarterly and sorted.
        
        ocf_ttm = df.groupby('ts_code')['n_cashflow_act'].rolling(4).sum().reset_index(level=0, drop=True)
        ni_ttm = df.groupby('ts_code')['n_income'].rolling(4).sum().reset_index(level=0, drop=True)
        
        factor_value = ocf_ttm / ni_ttm
        
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
