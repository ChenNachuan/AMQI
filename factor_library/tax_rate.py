
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

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
        
        # TTM Income Tax
        tax_ttm = df.groupby('ts_code')['income_tax'].rolling(4).sum().reset_index(level=0, drop=True)
        
        # TTM Total Profit
        profit_ttm = df.groupby('ts_code')['total_profit'].rolling(4).sum().reset_index(level=0, drop=True)
        
        factor_value = tax_ttm / profit_ttm
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
