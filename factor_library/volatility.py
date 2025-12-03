
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class Ivff(BaseFactor):
    """
    Idiosyncratic Volatility (IVFF).
    Standard deviation of residuals from Market Model: R_i = alpha + beta * R_m + epsilon.
    Calculated as sqrt(Var(R_i) - beta^2 * Var(R_m)).
    """
    
    @property
    def name(self) -> str:
        return "IVFF"
        
    @property
    def required_fields(self) -> list:
        return ['ret', 'mkt_ret', 'beta']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate IVFF using vectorized operations.
        """
        self.check_dependencies(df)
        
        # 1. Pivot Returns to Wide Format
        returns_wide = df.pivot(index='trade_date', columns='ts_code', values='ret')
        
        # 2. Get Market Return Series
        mkt_ret_df = df[['trade_date', 'mkt_ret']].drop_duplicates(subset=['trade_date'])
        mkt_ret_series = mkt_ret_df.set_index('trade_date')['mkt_ret']
        mkt_ret_series = mkt_ret_series.reindex(returns_wide.index)
        
        # 3. Pivot Beta (if available)
        # If beta is not in df, we can't calculate IVFF without it.
        # Assuming beta is passed in df as per requirement.
        if 'beta' not in df.columns:
            raise ValueError("Beta column is required for IVFF calculation.")
            
        beta_wide = df.pivot(index='trade_date', columns='ts_code', values='beta')
        
        window = 20
        
        # 4. Calculate Rolling Variance of Stock Returns (Vectorized)
        rolling_var_r = returns_wide.rolling(window).var()
        
        # 5. Calculate Rolling Variance of Market Returns
        rolling_var_rm = mkt_ret_series.rolling(window).var()
        
        # 6. Calculate Idiosyncratic Variance
        # Var(eps) = Var(R) - beta^2 * Var(Rm)
        # Broadcast subtraction and multiplication
        # beta^2 * Var(Rm) -> Multiply beta_wide squared by rolling_var_rm (column-wise broadcast)
        
        # Align rolling_var_rm to wide shape for broadcasting if needed, but pandas handles Series to DataFrame op
        term2 = beta_wide.pow(2).multiply(rolling_var_rm, axis=0)
        
        ivar = rolling_var_r - term2
        
        # Handle negative variance due to noise
        ivar = ivar.clip(lower=0)
        
        ivff_wide = np.sqrt(ivar)
        
        # 7. Stack back to Long Format
        ivff_long = ivff_wide.stack().reset_index()
        ivff_long.columns = ['trade_date', 'ts_code', self.name]
        
        # Result
        result = ivff_long.sort_values(['trade_date', 'ts_code'])
        result = result.set_index(['trade_date', 'ts_code'])
        
        return result
