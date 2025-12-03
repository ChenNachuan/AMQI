
import numpy as np
import pandas as pd
import statsmodels.api as sm

def annualized_return(series: pd.Series, periods_per_year: int = 12) -> float:
    """
    Calculate annualized return from periodic returns.
    Assumes series contains simple returns (e.g., 0.05 for 5%).
    """
    if len(series) == 0:
        return np.nan
    
    # Compound return
    total_return = (1 + series).prod() - 1
    n_periods = len(series)
    
    # Annualize
    ann_ret = (1 + total_return) ** (periods_per_year / n_periods) - 1
    return ann_ret

def annualized_volatility(series: pd.Series, periods_per_year: int = 12) -> float:
    """
    Calculate annualized volatility.
    """
    if len(series) < 2:
        return np.nan
    
    return series.std() * np.sqrt(periods_per_year)

def sharpe_ratio(series: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 12) -> float:
    """
    Calculate Sharpe Ratio.
    """
    if len(series) < 2:
        return np.nan
        
    excess_ret = series - risk_free_rate
    ann_ret = annualized_return(excess_ret, periods_per_year) + risk_free_rate # Approx
    # Better: mean excess return * periods / vol
    
    mean_excess = excess_ret.mean() * periods_per_year
    vol = annualized_volatility(series, periods_per_year)
    
    if vol == 0:
        return np.nan
        
    return mean_excess / vol

def max_drawdown(series: pd.Series) -> float:
    """
    Calculate Maximum Drawdown.
    """
    if len(series) == 0:
        return np.nan
        
    # Cumulative return index
    cum_ret = (1 + series).cumprod()
    running_max = cum_ret.cummax()
    drawdown = (cum_ret - running_max) / running_max
    return drawdown.min()

def calmar_ratio(series: pd.Series, periods_per_year: int = 12) -> float:
    """
    Calculate Calmar Ratio (Annualized Return / Max Drawdown).
    """
    ann_ret = annualized_return(series, periods_per_year)
    mdd = max_drawdown(series)
    
    if mdd == 0:
        return np.nan
        
    return ann_ret / abs(mdd)

def win_rate(series: pd.Series) -> float:
    """
    Calculate Win Rate (Percentage of positive periods).
    """
    if len(series) == 0:
        return np.nan
        
    return (series > 0).mean()

def newey_west_t_stat(series: pd.Series, lags: int = 6) -> float:
    """
    Calculate t-statistic with Newey-West adjustment for serial correlation.
    Used for Fama-MacBeth regression coefficients.
    
    Args:
        series: Time series of coefficients (e.g., factor premiums).
        lags: Number of lags for adjustment (default 6).
        
    Returns:
        t-statistic.
    """
    if len(series) < lags + 1:
        return np.nan
        
    # Regress on constant to get mean and SE
    X = np.ones(len(series))
    model = sm.OLS(series, X)
    results = model.fit(cov_type='HAC', cov_kwds={'maxlags': lags})
    
    return results.tvalues.iloc[0]
