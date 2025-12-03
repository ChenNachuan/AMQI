
import pandas as pd
import numpy as np
import statsmodels.api as sm
from .metrics import newey_west_t_stat

class FactorAnalyzer:
    def __init__(self, df: pd.DataFrame, factor_name: str, target_col: str = 'next_ret'):
        """
        Args:
            df: DataFrame indexed by [trade_date, ts_code].
            factor_name: Name of the factor column.
            target_col: Name of the target return column (default 'next_ret').
        """
        self.df = df.copy()
        self.factor_name = factor_name
        self.target_col = target_col
        
        # Ensure sorted
        self.df = self.df.sort_index()
        
    def calc_ic(self) -> dict:
        """
        Calculate Information Coefficient (IC) metrics.
        Returns:
            Dict with IC_Mean, IC_Std, ICIR, Rank_IC_Series.
        """
        # Rank IC per day/month
        # Group by trade_date
        # Use spearman correlation
        
        def ic_func(g):
            if len(g) < 10: # Min observations
                return np.nan
            return g[self.factor_name].corr(g[self.target_col], method='spearman')
            
        ic_series = self.df.groupby(level='trade_date').apply(ic_func)
        
        ic_mean = ic_series.mean()
        ic_std = ic_series.std()
        icir = ic_mean / ic_std if ic_std != 0 else np.nan
        
        # Autocorrelation (Turnover Proxy)
        # We need to pivot to get factor values matrix: index=date, col=stock
        factor_matrix = self.df[self.factor_name].unstack()
        # Corr between t and t-1
        # Use groupby level='ts_code' for robust calculation if unstack is heavy, but unstack is standard for matrix ops.
        # The user requested: self.df[self.factor_name].groupby('ts_code').apply(lambda x: x.autocorr(1)).mean()
        # But 'ts_code' is in index. So we use level='ts_code'.
        try:
            auto_corr = self.df[self.factor_name].groupby(level='ts_code').apply(lambda x: x.autocorr(1)).mean()
        except:
            auto_corr = np.nan
        
        return {
            'IC_Mean': ic_mean,
            'IC_Std': ic_std,
            'ICIR': icir,
            'IC_Series': ic_series,
            'Autocorrelation': auto_corr
        }
        
    def calc_turnover(self, quantiles: int = 5) -> float:
        """
        Calculate Turnover for the Long Portfolio (Q5).
        Formula: 1 - (Intersection of Q5_t and Q5_{t-1}) / Q5_t
        """
        # Ensure quantiles are assigned
        if 'quantile' not in self.df.columns:
            self.calc_factor_returns(quantiles=quantiles)
            
        # Filter for Q5
        q5_df = self.df[self.df['quantile'] == quantiles].reset_index()
        
        # Group by date and get set of stocks
        q5_sets = q5_df.groupby('trade_date')['ts_code'].apply(set)
        
        # Calculate turnover per period
        turnover_series = []
        dates = q5_sets.index.sort_values()
        
        for i in range(1, len(dates)):
            t = dates[i]
            t_prev = dates[i-1]
            
            set_t = q5_sets[t]
            set_prev = q5_sets[t_prev]
            
            if len(set_t) == 0:
                continue
                
            retention = len(set_t.intersection(set_prev)) / len(set_t)
            turnover = 1 - retention
            turnover_series.append(turnover)
            
        return np.mean(turnover_series) if turnover_series else np.nan

    def calc_factor_returns(self, weighting: str = 'vw', quantiles: int = 5, direction: str = 'positive') -> dict:
        """
        Calculate Portfolio Sorting returns.
        
        Args:
            weighting: 'vw' (value-weighted) or 'ew' (equal-weighted).
            quantiles: Number of buckets (default 5).
            direction: 'positive' (Long Q5, Short Q1) or 'negative' (Long Q1, Short Q5).
            
        Returns:
            Dict with 'quintile_returns' (DataFrame) and 'ls_returns' (Series).
        """
        # 1. Assign Quantiles
        def quantile_func(g):
            valid_count = g[self.factor_name].count()
            if valid_count < quantiles: # Not enough valid data
                return pd.Series(np.nan, index=g.index)
            try:
                return pd.qcut(g[self.factor_name], quantiles, labels=False, duplicates='drop') + 1
            except ValueError:
                return pd.Series(np.nan, index=g.index)
                
        self.df['quantile'] = self.df.groupby(level='trade_date').apply(quantile_func).reset_index(level=0, drop=True)
        
        # 2. Calculate Returns per Quantile
        # We need weights
        if weighting == 'vw':
            if 'size' in self.df.columns: 
                # 'size' is likely raw market cap (or proportional).
                # Check if it looks like log (small values < 50) or raw (large values > 1000)
                # But based on debugging, it's raw (e.g. 1e5).
                self.df['weight'] = self.df['size']
            else:
                # Fallback to EW if size not found
                self.df['weight'] = 1.0
        else:
            self.df['weight'] = 1.0
            
        # Weighted Average Return function
        def w_avg(g):
            # Filter for valid data
            valid = g.dropna(subset=[self.target_col, 'weight'])
            if len(valid) == 0 or valid['weight'].sum() == 0:
                return np.nan
            
            try:
                return np.average(valid[self.target_col], weights=valid['weight'])
            except Exception:
                return np.nan
            
        quintile_rets = self.df.groupby(['trade_date', 'quantile']).apply(w_avg).unstack()
        
        # 3. Long-Short Return
        # Check if we have Q1 and Q5
        q_min = 1
        q_max = quantiles
        
        # Handle column names being float or int
        cols = quintile_rets.columns
        if not cols.empty:
             q_min = cols.min()
             q_max = cols.max()
        
        if q_min in cols and q_max in cols:
            if direction == 'positive':
                # Long Q5, Short Q1
                ls_ret = quintile_rets[q_max] - quintile_rets[q_min]
            else:
                # Long Q1, Short Q5
                ls_ret = quintile_rets[q_min] - quintile_rets[q_max]
        else:
            print(f"Warning: Missing quantiles in returns. Columns: {quintile_rets.columns}")
            ls_ret = pd.Series(0.0, index=quintile_rets.index) # Return 0s instead of NaN to avoid crash, but warn
            
        return {
            'quintile_returns': quintile_rets,
            'ls_returns': ls_ret
        }
        
    def run_fama_macbeth(self) -> dict:
        """
        Run Fama-MacBeth Regression.
        Cross-sectional regression of R_{t+1} on Factor_t.
        """
        # Group by date
        def reg_func(g):
            if len(g) < 10:
                return np.nan
            # Standardize factor for comparison? Usually yes, but raw is also fine for premium.
            # Let's use raw to get "return per unit of factor".
            # Or standardize to get "return per 1 std of factor".
            # Let's standardize cross-sectionally.
            X = (g[self.factor_name] - g[self.factor_name].mean()) / g[self.factor_name].std()
            y = g[self.target_col]
            X = sm.add_constant(X)
            try:
                model = sm.OLS(y, X, missing='drop').fit()
                return model.params[self.factor_name]
            except:
                return np.nan
                
        fm_series = self.df.groupby(level='trade_date').apply(reg_func)
        
        # Time-series mean and t-stat
        fm_mean = fm_series.mean()
        fm_t = newey_west_t_stat(fm_series.dropna())
        
        return {
            'FM_Premium': fm_mean,
            'FM_t_stat': fm_t,
            'FM_Series': fm_series
        }
        
    def calc_alpha_beta(self, ls_returns: pd.Series, mkt_returns: pd.Series = None) -> dict:
        """
        Calculate CAPM Alpha and Beta.
        Args:
            ls_returns: Long-Short return series.
            mkt_returns: Market return series (optional). If None, will try to infer or use mean.
        """
        # If mkt_returns is None, we can't calculate CAPM Alpha properly.
        # But for now, let's assume we can get it or just return constant alpha.
        # Ideally, we should pass market returns.
        # In the engine, we can try to get market returns from the dataset (average of all stocks?).
        
        if mkt_returns is None:
            # Proxy market return as average of all stocks in universe
            mkt_returns = self.df.groupby(level='trade_date')[self.target_col].mean()
            
        # Align
        common_idx = ls_returns.index.intersection(mkt_returns.index)
        y = ls_returns.loc[common_idx]
        X = mkt_returns.loc[common_idx]
        X = sm.add_constant(X)
        
        try:
            model = sm.OLS(y, X, missing='drop').fit()
            alpha = model.params['const']
            beta = model.params.iloc[1] # Slope (2nd param)
            return {'Alpha': alpha, 'Beta': beta}
        except:
            return {'Alpha': np.nan, 'Beta': np.nan}
    def calc_daily_factor_returns(self, daily_df: pd.DataFrame, weighting: str = 'vw', quantiles: int = 5, direction: str = 'positive') -> dict:
        """
        Calculate Daily Portfolio Returns with Monthly Rebalancing.
        
        Args:
            daily_df: Daily DataFrame with [ts_code, trade_date, pct_chg].
            weighting: 'vw' or 'ew'.
            quantiles: Number of buckets.
            direction: 'positive' or 'negative'.
            
        Returns:
            Dict with 'quintile_daily_returns' and 'ls_daily_returns'.
        """
        # 1. Assign Quantiles (Monthly)
        # Reuse logic from calc_factor_returns but only need the mapping
        def quantile_func(g):
            valid_count = g[self.factor_name].count()
            if valid_count < quantiles:
                return pd.Series(np.nan, index=g.index)
            try:
                return pd.qcut(g[self.factor_name], quantiles, labels=False, duplicates='drop') + 1
            except ValueError:
                return pd.Series(np.nan, index=g.index)
                
        # Calculate quantiles on the monthly factor data
        monthly_quantiles = self.df.groupby(level='trade_date').apply(quantile_func).reset_index(level=0, drop=True)
        
        # Create holdings DataFrame: [trade_date, ts_code, quantile, weight]
        holdings = self.df.copy()
        holdings['quantile'] = monthly_quantiles
        
        # Calculate weights
        if weighting == 'vw' and 'size' in holdings.columns:
            holdings['weight'] = holdings['size']
        else:
            holdings['weight'] = 1.0
            
        # Filter for valid holdings
        holdings = holdings.dropna(subset=['quantile', 'weight']).reset_index()
        # holdings has 'trade_date' which is the rebalancing date (end of month)
        
        # 2. Prepare Daily Data
        # Ensure daily_df has required columns
        if 'pct_chg' not in daily_df.columns:
            # Try to calc from close if needed, but usually daily_adj has it or we computed it
            if 'close' in daily_df.columns:
                daily_df['pct_chg'] = daily_df.groupby('ts_code')['close'].pct_change()
            else:
                raise ValueError("daily_df must have 'pct_chg' or 'close'")
                
        daily_data = daily_df[['trade_date', 'ts_code', 'pct_chg']].copy()
        daily_data['trade_date'] = pd.to_datetime(daily_data['trade_date'])
        daily_data = daily_data.sort_values('trade_date')
        
        # 3. Merge Holdings with Daily Data
        # We want to use the holdings from the *previous* month end for the current day
        # e.g. Holdings at 2010-01-31 are used for 2010-02-01 to 2010-02-28
        
        # Sort holdings by date
        holdings['trade_date'] = pd.to_datetime(holdings['trade_date'])
        holdings = holdings.sort_values('trade_date')
        
        # Use merge_asof
        # daily_data is 'left', holdings is 'right'
        # We match daily_data.trade_date with holdings.trade_date
        # direction='backward' means we look for the closest holding date <= daily date
        # But wait, we rebalance at close of month M. So for month M+1, we use holdings of M.
        # The rebalancing date is M_end.
        # So for any day D > M_end, we use M_end holdings.
        # But we must strictly exclude D == M_end (because on rebalancing day we haven't rebalanced yet? 
        # Actually standard backtest assumes we trade at close of M_end, so M_end+1 uses new weights).
        # merge_asof with 'backward' finds closest date <= current.
        # If we use allow_exact_matches=False, it finds < current.
        # If daily date is Feb 1, and holdings are Jan 31, backward finds Jan 31. Correct.
        # If daily date is Jan 31, backward finds Jan 31. 
        # But on Jan 31 we should use Jan 31 holdings? No, on Jan 31 we use Dec 31 holdings.
        # So we need strict inequality: holdings_date < daily_date.
        # merge_asof doesn't support strict inequality directly easily without allow_exact_matches=False.
        
        merged = pd.merge_asof(
            daily_data.sort_values('trade_date'),
            holdings[['trade_date', 'ts_code', 'quantile', 'weight']].sort_values('trade_date'),
            on='trade_date',
            by='ts_code',
            direction='backward',
            allow_exact_matches=False 
        )
        
        # Drop rows where we didn't find holdings (e.g. before first rebalancing)
        merged = merged.dropna(subset=['quantile'])
        
        # 4. Calculate Daily Portfolio Returns
        # Group by [daily_date, quantile]
        def w_avg(g):
            if g['weight'].sum() == 0:
                return np.nan
            return np.average(g['pct_chg'], weights=g['weight'])
            
        # pct_chg in daily_adj is usually 0-100 or 0-1? 
        # Tushare usually gives 0-100? Need to check.
        # Assuming 0.01 format (1%). If it's 100, we need to divide by 100.
        # Let's check max value. If > 1, likely percentage.
        if merged['pct_chg'].abs().max() > 1.5: # Heuristic
             merged['pct_chg'] = merged['pct_chg'] / 100.0
             
        daily_quintile_rets = merged.groupby(['trade_date', 'quantile']).apply(w_avg).unstack()
        
        # 5. Long-Short Daily Returns
        q_min = 1
        q_max = quantiles
        cols = daily_quintile_rets.columns
        if not cols.empty:
             q_min = cols.min()
             q_max = cols.max()
             
        if q_min in cols and q_max in cols:
            if direction == 'positive':
                ls_daily = daily_quintile_rets[q_max] - daily_quintile_rets[q_min]
            else:
                ls_daily = daily_quintile_rets[q_min] - daily_quintile_rets[q_max]
        else:
            ls_daily = pd.Series(0.0, index=daily_quintile_rets.index)
            
        return {
            'quintile_daily_returns': daily_quintile_rets,
            'ls_daily_returns': ls_daily
        }
