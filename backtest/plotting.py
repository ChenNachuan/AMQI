
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def plot_cumulative_returns(quintile_returns: pd.DataFrame, ls_returns: pd.Series):
    """
    Plot cumulative returns of Q1, Q5, and Long-Short.
    """
    plt.figure(figsize=(12, 6))
    
    # Calculate cumulative returns
    cum_q1 = (1 + quintile_returns[1]).cumprod()
    cum_q5 = (1 + quintile_returns[quintile_returns.columns[-1]]).cumprod()
    cum_ls = (1 + ls_returns).cumprod()
    
    plt.plot(cum_q1.index, cum_q1, label='Q1 (Low)', linestyle='--')
    plt.plot(cum_q5.index, cum_q5, label='Q5 (High)')
    plt.plot(cum_ls.index, cum_ls, label='Long-Short', color='red', linewidth=2)
    
    plt.title('Cumulative Returns: Q1 vs Q5 vs Long-Short')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Return (1.0 = Base)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()

def plot_ic_series(ic_series: pd.Series):
    """
    Plot IC time series (Bar chart).
    """
    plt.figure(figsize=(12, 4))
    
    # Color bars: Positive green, Negative red
    colors = ['green' if x > 0 else 'red' for x in ic_series]
    
    plt.bar(ic_series.index, ic_series, color=colors, width=20) # Width depends on frequency
    
    plt.axhline(0, color='black', linewidth=0.5)
    plt.axhline(ic_series.mean(), color='blue', linestyle='--', label=f'Mean IC: {ic_series.mean():.3f}')
    
    plt.title('Information Coefficient (IC) Time Series')
    plt.xlabel('Date')
    plt.ylabel('Rank IC')
    plt.legend()
    plt.show()

def plot_quantile_bar(quintile_returns: pd.DataFrame):
    """
    Plot average annualized return per quantile.
    """
    # Calculate annualized return for each quantile
    # Simple approximation: mean * 12
    ann_rets = quintile_returns.mean() * 12
    
    plt.figure(figsize=(8, 5))
    sns.barplot(x=ann_rets.index, y=ann_rets.values, palette='viridis')
    
    plt.title('Annualized Return by Quantile')
    plt.xlabel('Quantile')
    plt.ylabel('Annualized Return')
    plt.grid(axis='y', alpha=0.3)
    plt.show()
