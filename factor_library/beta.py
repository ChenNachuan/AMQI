
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
        
        # Vectorized Beta Calculation
        # 1. Pivot Returns to Wide Format (Index=Date, Columns=Stock)
        # This handles alignment automatically
        returns_wide = df.pivot(index='trade_date', columns='ts_code', values='ret')
        
        # 2. Get Market Return Series
        # Since mkt_ret is repeated for each stock, we can just take the mean across columns or pick one valid column
        # But wait, mkt_ret might be missing for some stocks if they are suspended?
        # Ideally, mkt_ret comes from a benchmark and is consistent.
        # Let's pivot mkt_ret as well to be safe, or just take the first valid one per date.
        # Actually, simpler: df[['trade_date', 'mkt_ret']].drop_duplicates().set_index('trade_date')
        mkt_ret_series = df[['trade_date', 'mkt_ret']].drop_duplicates().set_index('trade_date')['mkt_ret']
        
        # Align market returns to the wide dataframe index
        mkt_ret_series = mkt_ret_series.reindex(returns_wide.index)
        
        window = 252
        
        # 3. Calculate Rolling Covariance (Vectorized)
        # df.rolling().cov(series) broadcasts the series to all columns
        rolling_cov = returns_wide.rolling(window).cov(mkt_ret_series)
        
        # 4. Calculate Rolling Variance of Market
        rolling_var = mkt_ret_series.rolling(window).var()
        
        # 5. Calculate Beta
        # Broadcast division
        beta_wide = rolling_cov.div(rolling_var, axis=0)
        
        # 6. Stack back to Long Format
        beta_long = beta_wide.stack().reset_index()
        beta_long.columns = ['trade_date', 'ts_code', self.name]
        
        # Result
        result = beta_long.sort_values(['trade_date', 'ts_code'])
        result = result.set_index(['trade_date', 'ts_code'])
        
        return result
