
import pandas as pd
import numpy as np
from .analyzer import FactorAnalyzer
from .metrics import annualized_return, annualized_volatility, sharpe_ratio, max_drawdown
from .plotting import plot_cumulative_returns, plot_ic_series, plot_quantile_bar

class BacktestEngine:
    def __init__(self, df: pd.DataFrame, factor_name: str, target_col: str = 'next_ret', benchmark_df: pd.DataFrame = None):
        """
        Initialize the Backtest Engine.
        Args:
            df: DataFrame containing factor and target returns.
            factor_name: Name of the factor to analyze.
            benchmark_df: Optional DataFrame with [trade_date, ret] for benchmark.
        """
        self.df = df
        self.factor_name = factor_name
        self.benchmark_df = benchmark_df
        
        # Check if factor exists, if not, try to calculate it
        if self.factor_name not in self.df.columns:
            print(f"在数据集中未找到因子 {self.factor_name}。尝试计算...")
            self._calculate_factor()
            
        self.analyzer = FactorAnalyzer(self.df, factor_name, target_col)
        self.results = {}
        
        # Load daily data for continuous plotting
        self.daily_df = None
        self._load_daily_data()
        
    def _load_daily_data(self):
        """
        Load daily adjusted data for daily return calculation.
        """
        import os
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        daily_path = os.path.join(base_dir, 'data', 'data_cleaner', 'daily_adj.parquet')
        
        if os.path.exists(daily_path):
            try:
                self.daily_df = pd.read_parquet(daily_path)
                # Ensure pct_chg exists
                if 'pct_chg' not in self.daily_df.columns:
                     if 'close' in self.daily_df.columns:
                         self.daily_df['pct_chg'] = self.daily_df.groupby('ts_code')['close'].pct_change()
            except Exception as e:
                print(f"Warning: Failed to load daily data from {daily_path}: {e}")
        else:
            print(f"Warning: Daily data not found at {daily_path}")
        
    def _calculate_factor(self):
        """
        Calculate factor on the fly using factor_library.
        """
        import factor_library
        
        # Mapping from short name to Class Name
        # Based on user request and implementation
        mapping = {
            'ATR': 'AverageTrueRange',
            'Boll': 'BollingerBands',
            'Ichimoku': 'Ichimoku',
            'MFI': 'MoneyFlowIndex',
            'OBV': 'OnBalanceVolume',
            'PVT': 'PriceVolumeTrend',
            'RVI': 'RelativeVigorIndex',
            'TEMA': 'TripleEMA',
            'SWMA': 'SineWMA',
            # Add others if needed
            'R11': 'Momentum', # Example
            'IVFF': 'Ivff',
            'beta': 'Beta',
            'TUR': 'Turnover',
            'Srev': 'Reversal'
        }
        
        class_name = mapping.get(self.factor_name)
        if not class_name:
            print(f"警告：未找到因子 {self.factor_name} 的映射。分析可能会失败。")
            return
            
        try:
            factor_cls = getattr(factor_library, class_name)
            factor = factor_cls()
            
            # Calculate
            # Note: factor.calculate returns a DataFrame with index [trade_date, ts_code] and one column
            print(f"正在计算 {class_name}...")
            factor_df = factor.calculate(self.df.reset_index())
            
            # Merge back to self.df
            # self.df is indexed by [trade_date, ts_code]
            # factor_df is also indexed by [trade_date, ts_code]
            
            # Check for duplicates in factor_df
            if factor_df.index.duplicated().any():
                print("警告：计算出的因子中存在重复索引。")
                factor_df = factor_df[~factor_df.index.duplicated(keep='first')]
                
            # Join
            # We use join to keep self.df structure
            # But we need to handle if column already exists (shouldn't happen due to check)
            
            # Rename column to self.factor_name if it's different (e.g. 'R11' vs 'Momentum')
            # The factor.calculate returns column with factor.name.
            # We need to map it to self.factor_name or update self.factor_name?
            # The analyzer uses self.factor_name.
            # So we should rename the result column to self.factor_name.
            
            res_col = factor.name
            if res_col != self.factor_name:
                print(f"正在重命名因子输出 {res_col} 为 {self.factor_name}")
                factor_df = factor_df.rename(columns={res_col: self.factor_name})
                
            self.df = self.df.join(factor_df[[self.factor_name]], how='left')
            print(f"因子 {self.factor_name} 已计算并添加。")
            
        except Exception as e:
            print(f"计算因子 {self.factor_name} 时出错：{e}")
            import traceback
            traceback.print_exc()
        
    def run_analysis(self, weighting: str = 'vw', direction: str = 'positive') -> dict:
        """
        Run the full analysis pipeline.
        Args:
            weighting: 'vw' (value-weighted) or 'ew' (equal-weighted).
            direction: 'positive' (Long Q5, Short Q1) or 'negative' (Long Q1, Short Q5).
        Returns:
            Summary dictionary.
        """
        print(f"正在运行因子分析：{self.factor_name} (方向: {direction})...")
        
        # 1. IC Analysis
        ic_metrics = self.analyzer.calc_ic()
        self.results['ic'] = ic_metrics
        
        # 2. Portfolio Sorting
        # 2. Portfolio Sorting
        sort_metrics = self.analyzer.calc_factor_returns(weighting=weighting, direction=direction)
        self.results['sorting'] = sort_metrics
        
        # 2.1 Daily Returns (for plotting)
        if self.daily_df is not None:
            print("Calculating daily portfolio returns...")
            daily_metrics = self.analyzer.calc_daily_factor_returns(
                self.daily_df, weighting=weighting, direction=direction
            )
            self.results['daily_sorting'] = daily_metrics
            # Use daily returns for performance metrics if available
            ls_ret = daily_metrics['ls_daily_returns']
            quintile_rets = daily_metrics['quintile_daily_returns']
            periods_per_year = 252
        else:
            print("Warning: Using monthly returns for performance metrics (discontinuous plot).")
            ls_ret = sort_metrics['ls_returns']
            quintile_rets = sort_metrics['quintile_returns']
            periods_per_year = 12
        
        # 3. Turnover (Long Portfolio)
        # If positive, Long Q5. If negative, Long Q1.
        long_q = 5 if direction == 'positive' else 1
        turnover = self.analyzer.calc_turnover(quantiles=long_q)
        
        # 4. Fama-MacBeth
        fm_metrics = self.analyzer.run_fama_macbeth()
        self.results['fm'] = fm_metrics
        
        # 5. Alpha/Beta (CAPM)
        # 5. Alpha/Beta (CAPM)
        # ls_ret is already defined above (daily or monthly)
        
        # Determine Benchmark Return
        if self.benchmark_df is not None:
            # Align benchmark
            mkt_ret = self.benchmark_df.set_index('trade_date')['ret']
        else:
            # Infer market return for CAPM (Universe Mean)
            mkt_ret = self.df.groupby(level='trade_date')['next_ret'].mean()
            
        capm_metrics = self.analyzer.calc_alpha_beta(ls_ret, mkt_ret)
        self.results['capm'] = capm_metrics
        
        # 6. Performance Metrics (Long-Short)
        # 6. Performance Metrics (Long-Short)
        ls_perf = {
            'Annualized Return': annualized_return(ls_ret, periods_per_year),
            'Sharpe Ratio': sharpe_ratio(ls_ret, periods_per_year=periods_per_year),
            'Max Drawdown': max_drawdown(ls_ret)
        }
        
        # 7. Long-Only Performance
        # If direction is positive, use max quantile (Q5). If negative, use min quantile (Q1).
        # 7. Long-Only Performance
        # quintile_rets is already defined above (daily or monthly)
        
        target_q = None
        if not quintile_rets.columns.empty:
            if direction == 'positive':
                target_q = quintile_rets.columns.max()
                q_label = f"Q{target_q}"
            else:
                target_q = quintile_rets.columns.min()
                q_label = f"Q{target_q}"
        else:
            q_label = "Q?"
        
        if target_q is not None and target_q in quintile_rets.columns:
            long_ret = quintile_rets[target_q]
            long_perf = {
                'Annualized Return': annualized_return(long_ret, periods_per_year),
                'Sharpe Ratio': sharpe_ratio(long_ret, periods_per_year=periods_per_year),
                'Max Drawdown': max_drawdown(long_ret)
            }
            
            # Active Return (Long - Benchmark)
            if self.benchmark_df is not None:
                # Align
                common = long_ret.index.intersection(mkt_ret.index)
                active_ret = long_ret.loc[common] - mkt_ret.loc[common]
                # Align
                common = long_ret.index.intersection(mkt_ret.index)
                active_ret = long_ret.loc[common] - mkt_ret.loc[common]
                # Note: mkt_ret might be monthly while long_ret is daily. 
                # If mismatch, we skip active return or need daily benchmark.
                # For now, if periods != 12, we assume daily.
                if periods_per_year == 252 and len(mkt_ret) < len(long_ret) * 0.5:
                     # Mismatch frequency
                     active_ann_ret = np.nan
                else:
                     active_ann_ret = annualized_return(active_ret, periods_per_year)
            else:
                active_ann_ret = np.nan
        else:
            long_perf = {
                'Annualized Return': np.nan,
                'Sharpe Ratio': np.nan,
                'Max Drawdown': np.nan
            }
            active_ann_ret = np.nan
        
        # Compile Summary
        summary = {
            # Factor Potency
            'IC均值': ic_metrics['IC_Mean'],
            'IC_IR': ic_metrics['ICIR'],
            '因子自相关性': ic_metrics['Autocorrelation'],
            
            # Long-Short Performance (Theoretical)
            '多空年化收益': ls_perf['Annualized Return'],
            '多空夏普比率': ls_perf['Sharpe Ratio'],
            'FM回归t值': fm_metrics['FM_t_stat'],
            
            # Long-Only Performance (Investable)
            f'{q_label}年化收益': long_perf['Annualized Return'],
            f'{q_label}夏普比率': long_perf['Sharpe Ratio'],
            f'{q_label}最大回撤': long_perf['Max Drawdown'],
            f'{q_label}换手率': turnover,
            f'{q_label}超额收益': active_ann_ret,
            
            # Risk Adjustment
            'Alpha': capm_metrics['Alpha'],
            'Beta': capm_metrics['Beta']
        }
        
        return summary
        
    def plot_results(self):
        """
        Visualize the results.
        """
        if 'sorting' not in self.results:
            print("请先运行 run_analysis()。")
            return
            
        # 1. Cumulative Returns
        bench_ret = None
        if self.benchmark_df is not None:
            bench_ret = self.benchmark_df.set_index('trade_date')['ret']
            
        # 1. Cumulative Returns
        bench_ret = None
        if self.benchmark_df is not None:
            bench_ret = self.benchmark_df.set_index('trade_date')['ret']
            
        # Use daily returns if available
        if 'daily_sorting' in self.results:
            plot_cumulative_returns(
                self.results['daily_sorting']['quintile_daily_returns'], 
                self.results['daily_sorting']['ls_daily_returns'],
                benchmark_returns=bench_ret # Note: Benchmark might need to be daily too
            )
        else:
            plot_cumulative_returns(
                self.results['sorting']['quintile_returns'], 
                self.results['sorting']['ls_returns'],
                benchmark_returns=bench_ret
            )
        
        # 2. IC Series
        plot_ic_series(self.results['ic']['IC_Series'])
        
        # 3. Quantile Bar
        plot_quantile_bar(self.results['sorting']['quintile_returns'])
