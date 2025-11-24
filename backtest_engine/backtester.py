
import pandas as pd
import numpy as np
from .metrics import calculate_sharpe_ratio, calculate_max_drawdown

class Backtester:
    def __init__(self, data: pd.DataFrame):
        """
        Args:
            data: Full dataset containing 'ret' and 'stkcd', 'year', 'month'.
        """
        self.data = data.copy()
        if 'stkcd' in self.data.columns:
             self.data = self.data.set_index(['stkcd', 'year', 'month'])

    def run_backtest(self, signal: pd.Series, quantiles: int = 5) -> dict:
        """
        Runs a simple quantile-based backtest.
        
        Args:
            signal: Factor values aligned with the data index.
            quantiles: Number of portfolios to sort into.

        Returns:
            dict: Performance metrics for the Long-Short portfolio.
        """
        # Align signal with returns
        df = self.data[['ret']].copy()
        df['signal'] = signal
        df = df.dropna()

        # Rank and assign quantiles per month
        def assign_quantile(x):
            try:
                return pd.qcut(x, quantiles, labels=False, duplicates='drop')
            except ValueError:
                return pd.Series(index=x.index, data=0) # Fallback if not enough data

        df['quantile'] = df.groupby(['year', 'month'])['signal'].transform(assign_quantile)

        # Calculate portfolio returns
        portfolio_returns = df.groupby(['year', 'month', 'quantile'])['ret'].mean().unstack()
        
        # Long-Short Strategy (High - Low)
        # Assuming High signal = High return. If opposite, swap.
        long_short_returns = portfolio_returns[quantiles-1] - portfolio_returns[0]
        
        stats = {
            'Sharpe Ratio': calculate_sharpe_ratio(long_short_returns),
            'Max Drawdown': calculate_max_drawdown(long_short_returns),
            'Mean Return': long_short_returns.mean() * 12, # Annualized
            'Volatility': long_short_returns.std() * np.sqrt(12) # Annualized
        }
        
        return stats
