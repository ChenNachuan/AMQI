
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class NOAT(BaseFactor):
    """
    Net Operating Asset Turnover.
    Revenue (TTM) / Average Net Operating Assets.
    NOA approx = Total Assets - Cash - Non-Interest Liabilities.
    Here approximated as: Total Assets - Cash (money_cap) - (Total Liab - Debt).
    Or simpler given fields: Total Assets - (Total Liab - Interest Debt).
    """
    
    @property
    def name(self) -> str:
        return "NOAT"
        
    @property
    def required_fields(self) -> list:
        return ['revenue', 'total_assets', 'total_liab', 'money_cap']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)
        
        # TTM Revenue
        rev_ttm = df.groupby('ts_code')['revenue'].rolling(4).sum().reset_index(level=0, drop=True)
        
        # Net Operating Assets
        # NOA = Operating Assets - Operating Liabilities
        # Operating Assets = Total Assets - Cash & Equivalents
        # Operating Liabilities = Total Liabilities - Total Debt
        # Since we might not have Total Debt, we can use a simpler proxy or try to use interestdebt if available.
        # User suggested: revenue, total_hldr_eqy_inc_min_int, total_assets, interestdebt.
        
        # Let's try to use the user suggested fields if possible.
        # If we have 'interestdebt', we can estimate Operating Liab = Total Liab - Interest Debt.
        # But we need Total Liab. Total Liab = Total Assets - Equity.
        
        total_liab = df['total_assets'] - df.get('total_hldr_eqy_inc_min_int', 0)
        
        # If interestdebt is available, use it. Else assume 0 or some proxy.
        debt = df.get('interestdebt', 0)
        
        # Operating Liab = Total Liab - Debt
        op_liab = total_liab - debt
        
        # Operating Assets = Total Assets - Cash
        cash = df.get('money_cap', 0)
        op_assets = df['total_assets'] - cash
        
        noa = op_assets - op_liab
        
        # Ensure numeric
        noa = pd.to_numeric(noa, errors='coerce')
        
        # Average NOA
        # We can assign noa to df temporarily
        # Assuming df is sorted by ts_code, end_date
        df['_noa'] = noa
        avg_noa = df.groupby('ts_code')['_noa'].rolling(4).mean().reset_index(level=0, drop=True)
        
        factor_value = rev_ttm / avg_noa
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)
        
        result = pd.DataFrame({
            self.name: factor_value,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })
        
        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']
            
        return result
