
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class APDays(BaseFactor):
    """
    Accounts Payable Days.
    360 / APTurnover.
    """
    
    @property
    def name(self) -> str:
        return "APDays"
        
    @property
    def required_fields(self) -> list:
        return ['total_cogs', 'accounts_payable']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        # TTM COGS
        cogs_ttm = df.groupby('ts_code')['total_cogs'].rolling(4).sum().reset_index(level=0, drop=True)
        
        # Average Accounts Payable
        avg_ap = df.groupby('ts_code')['accounts_payable'].rolling(4).mean().reset_index(level=0, drop=True)
        
        ap_turnover = cogs_ttm / avg_ap
        
        factor_value = 360 / ap_turnover
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
