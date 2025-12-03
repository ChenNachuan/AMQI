
import pandas as pd
import numpy as np
from .base_factor import BaseFactor
from scripts.utils.financial_utils import convert_ytd_to_ttm

class APTurnover(BaseFactor):
    """
    Accounts Payable Turnover.
    COGS (TTM) / Average Accounts Payable.
    """
    
    @property
    def name(self) -> str:
        return "APTurnover"
        
    @property
    def required_fields(self) -> list:
        # COGS might be 'total_cogs' or 'oper_cost' depending on data source. 
        # User specified 'total_cogs'.
        return ['total_cogs', 'acct_payable']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        # Calculate TTM for COGS (Flow variable)
        # Already converted to TTM in construct_fundamental_factors.py
        
        # Average Accounts Payable (Stock variable)
        # Using rolling mean of last 4 quarters to represent the average level during the TTM period
        avg_ap = df.groupby('ts_code')['acct_payable'].rolling(4).mean().reset_index(level=0, drop=True)
        
        factor_value = df['total_cogs'] / avg_ap
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
