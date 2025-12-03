
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

    def calc_factor_returns(self, weighting: str = 'vw', quantiles: int = 5) -> dict:
        """
        Calculate Portfolio Sorting returns.
        
        Args:
            weighting: 'vw' (value-weighted) or 'ew' (equal-weighted).
            quantiles: Number of buckets (default 5).
            
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
        # Q5 - Q1
        # Check if we have Q1 and Q5
        if 1.0 in quintile_rets.columns and float(quantiles) in quintile_rets.columns:
            ls_ret = quintile_rets[float(quantiles)] - quintile_rets[1.0]
        elif 1 in quintile_rets.columns and quantiles in quintile_rets.columns:
             ls_ret = quintile_rets[quantiles] - quintile_rets[1]
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
