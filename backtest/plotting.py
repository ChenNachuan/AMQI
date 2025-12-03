
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Set Chinese font
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'PingFang SC']
plt.rcParams['axes.unicode_minus'] = False

def plot_cumulative_returns(quintile_returns: pd.DataFrame, ls_returns: pd.Series, benchmark_returns: pd.Series = None):
    """
    Plot cumulative returns of Q1, Q5, Long-Short, and Benchmark.
    """
    plt.figure(figsize=(12, 6))
    
    # Calculate cumulative returns
    # Fill NaNs with 0 (assuming cash) to avoid breaking the plot
    
    # Robustly get Q1 and Q5 (or min/max available)
    cols = quintile_returns.columns
    if cols.empty:
        print("Warning: No quantile returns to plot.")
        return

    min_q = cols.min()
    max_q = cols.max()
    
    cum_q1 = (1 + quintile_returns[min_q].fillna(0)).cumprod()
    cum_q5 = (1 + quintile_returns[max_q].fillna(0)).cumprod()
    cum_ls = (1 + ls_returns.fillna(0)).cumprod()
    
    plt.plot(cum_q1.index, cum_q1, label=f'Q{min_q} (低)', linestyle='--')
    plt.plot(cum_q5.index, cum_q5, label=f'Q{max_q} (高)')
    plt.plot(cum_ls.index, cum_ls, label='多空', color='red', linewidth=2)
    
    if benchmark_returns is not None:
        # Align benchmark with other returns
        common_idx = cum_ls.index.intersection(benchmark_returns.index)
        if not common_idx.empty:
            bench_aligned = benchmark_returns.loc[common_idx].fillna(0)
            cum_bench = (1 + bench_aligned).cumprod()
            plt.plot(cum_bench.index, cum_bench, label='基准 (沪深300)', color='black', linestyle='-.', alpha=0.7)
    
    plt.title('累计收益率：Q1 vs Q5 vs 多空 vs 基准')
    plt.xlabel('日期')
    plt.ylabel('累计收益 (1.0 = 基数)')
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
    plt.axhline(ic_series.mean(), color='blue', linestyle='--', label=f'平均 IC: {ic_series.mean():.3f}')
    
    plt.title('信息系数 (IC) 时间序列')
    plt.xlabel('日期')
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
    
    plt.title('分位数年化收益率')
    plt.xlabel('分位数')
    plt.ylabel('年化收益率')
    plt.grid(axis='y', alpha=0.3)
    plt.show()
