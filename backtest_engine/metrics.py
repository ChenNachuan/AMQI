
import pandas as pd
import numpy as np

def calculate_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    """
    Calculates the annualized Sharpe Ratio.
    Assumes monthly returns.
    """
    excess_returns = returns - risk_free_rate
    if excess_returns.std() == 0:
        return 0.0
    return (excess_returns.mean() / excess_returns.std()) * np.sqrt(12)

def calculate_max_drawdown(returns: pd.Series) -> float:
    """
    Calculates the maximum drawdown.
    """
    cumulative_returns = (1 + returns).cumprod()
    peak = cumulative_returns.cummax()
    drawdown = (cumulative_returns - peak) / peak
    return drawdown.min()

def calculate_ic(factor_values: pd.Series, forward_returns: pd.Series) -> float:
    """
    Calculates the Information Coefficient (IC).
    """
    # Align data
    data = pd.concat([factor_values, forward_returns], axis=1).dropna()
    if data.empty:
        return 0.0
    return data.iloc[:, 0].corr(data.iloc[:, 1])
