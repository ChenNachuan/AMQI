
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class Beta(BaseFactor):
    """
    CAPM Beta Factor.
    Calculated as Cov(R_i, R_m) / Var(R_m) over a rolling window.
    """
    
    @property
    def name(self) -> str:
        return "beta" # Lowercase to match column name expectation in Ivff
        
    @property
    def required_fields(self) -> list:
        return ['ret', 'mkt_ret']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Beta.
        
        Args:
            df: DataFrame with 'ret' and 'mkt_ret'.
            
        Returns:
            DataFrame with 'beta' column.
        """
        self.check_dependencies(df)
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 252 # 1 year rolling window
        
        # Calculate Rolling Covariance and Variance
        # We need rolling Cov(R_i, R_m)
        
        # Group by ts_code
        grouped = df.groupby('ts_code')
        
        # Rolling Covariance
        # pandas rolling cov requires two series.
        # We can do: df.groupby('ts_code').apply(lambda x: x['ret'].rolling(window).cov(x['mkt_ret']))
        # But apply is slow.
        # Optimization:
        # cov(x, y) = E[xy] - E[x]E[y] (approx)
        # Or just use the rolling().cov() which is optimized in recent pandas.
        
        cov_rim = grouped.apply(lambda x: x['ret'].rolling(window).cov(x['mkt_ret'])).reset_index(0, drop=True)
        
        # Rolling Variance of Market
        # var_rm = grouped['mkt_ret'].rolling(window).var() 
        # Since mkt_ret is same for all, we can just do it once? 
        # No, because the window must align with the stock's dates (suspensions etc).
        # So best to do it per group to handle missing dates correctly if any.
        var_rm = grouped['mkt_ret'].rolling(window).var().reset_index(0, drop=True)
        
        beta = cov_rim / var_rm
        
        # Result
        result = pd.DataFrame({
            self.name: beta,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
