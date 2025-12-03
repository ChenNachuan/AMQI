
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
        Calculate Beta using vectorized operations.
        """
        self.check_dependencies(df)
        
        # Ensure unique index for pivot
        # If there are duplicates, we might need to handle them. Assuming clean data for now.
        # Pivot Returns: Index=Date, Columns=Stock
        returns_wide = df.pivot(index='trade_date', columns='ts_code', values='ret')
        
        # Get Market Return Series (assuming consistent across stocks for same date)
        # We take the mean across stocks for mkt_ret to handle potential missing data or just pick one.
        # Faster: drop_duplicates
        mkt_ret_df = df[['trade_date', 'mkt_ret']].drop_duplicates(subset=['trade_date'])
        mkt_ret_series = mkt_ret_df.set_index('trade_date')['mkt_ret']
        
        # Align market returns to the wide dataframe index
        mkt_ret_series = mkt_ret_series.reindex(returns_wide.index)
        
        window = 252
        
        # Calculate Rolling Covariance (Vectorized)
        # df.rolling().cov(series) broadcasts the series to all columns
        rolling_cov = returns_wide.rolling(window).cov(mkt_ret_series)
        
        # Calculate Rolling Variance of Market
        rolling_var = mkt_ret_series.rolling(window).var()
        
        # Calculate Beta
        # Broadcast division: Divide each column of covariance by the market variance series
        beta_wide = rolling_cov.div(rolling_var, axis=0)
        
        # Stack back to Long Format
        beta_long = beta_wide.stack().reset_index()
        beta_long.columns = ['trade_date', 'ts_code', self.name]
        
        # Result
        result = beta_long.sort_values(['trade_date', 'ts_code'])
        result = result.set_index(['trade_date', 'ts_code'])
        
        return result
