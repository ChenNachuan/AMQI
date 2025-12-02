
import pandas as pd
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
        
    def run_analysis(self, weighting: str = 'vw') -> dict:
        """
        Run the full analysis pipeline.
        Args:
            weighting: 'vw' (value-weighted) or 'ew' (equal-weighted).
        Returns:
            Summary dictionary.
        """
        print(f"正在运行因子分析：{self.factor_name}...")
        
        # 1. IC Analysis
        ic_metrics = self.analyzer.calc_ic()
        self.results['ic'] = ic_metrics
        
        # 2. Portfolio Sorting
        sort_metrics = self.analyzer.calc_factor_returns(weighting=weighting)
        self.results['sorting'] = sort_metrics
        
        # 3. Turnover (Q5)
        turnover = self.analyzer.calc_turnover(quantiles=5)
        
        # 4. Fama-MacBeth
        fm_metrics = self.analyzer.run_fama_macbeth()
        self.results['fm'] = fm_metrics
        
        # 5. Alpha/Beta (CAPM)
        ls_ret = sort_metrics['ls_returns']
        
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
        ls_perf = {
            'Annualized Return': annualized_return(ls_ret),
            'Sharpe Ratio': sharpe_ratio(ls_ret),
            'Max Drawdown': max_drawdown(ls_ret)
        }
        
        # 7. Long-Only Performance (Q5)
        # Assuming Q5 is the Top Quantile (High Factor Value)
        # If factor is inverse (e.g. volatility), user might want Q1. 
        # But standard is Q5.
        q5_ret = sort_metrics['quintile_returns'][5.0]
        q5_perf = {
            'Annualized Return': annualized_return(q5_ret),
            'Sharpe Ratio': sharpe_ratio(q5_ret),
            'Max Drawdown': max_drawdown(q5_ret)
        }
        
        # Active Return (Q5 - Benchmark)
        if self.benchmark_df is not None:
            # Align
            common = q5_ret.index.intersection(mkt_ret.index)
            active_ret = q5_ret.loc[common] - mkt_ret.loc[common]
            active_ann_ret = annualized_return(active_ret)
        else:
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
            'Q5年化收益': q5_perf['Annualized Return'],
            'Q5夏普比率': q5_perf['Sharpe Ratio'],
            'Q5最大回撤': q5_perf['Max Drawdown'],
            'Q5换手率': turnover,
            'Q5超额收益': active_ann_ret,
            
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
            
        plot_cumulative_returns(
            self.results['sorting']['quintile_returns'], 
            self.results['sorting']['ls_returns'],
            benchmark_returns=bench_ret
        )
        
        # 2. IC Series
        plot_ic_series(self.results['ic']['IC_Series'])
        
        # 3. Quantile Bar
        plot_quantile_bar(self.results['sorting']['quintile_returns'])
