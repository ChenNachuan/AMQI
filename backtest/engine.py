
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
        self.analyzer = FactorAnalyzer(df, factor_name, target_col)
        self.results = {}
        
    def run_analysis(self, weighting: str = 'vw') -> dict:
        """
        Run the full analysis pipeline.
        Args:
            weighting: 'vw' (value-weighted) or 'ew' (equal-weighted).
        Returns:
            Summary dictionary.
        """
        print(f"Running analysis for factor: {self.factor_name}...")
        
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
            'IC_Mean': ic_metrics['IC_Mean'],
            'IC_IR': ic_metrics['ICIR'],
            'Factor_Autocorr': ic_metrics['Autocorrelation'],
            
            # Long-Short Performance (Theoretical)
            'LS_Return': ls_perf['Annualized Return'],
            'LS_Sharpe': ls_perf['Sharpe Ratio'],
            'FM_t_stat': fm_metrics['FM_t_stat'],
            
            # Long-Only Performance (Investable)
            'Q5_Return': q5_perf['Annualized Return'],
            'Q5_Sharpe': q5_perf['Sharpe Ratio'],
            'Q5_MaxDD': q5_perf['Max Drawdown'],
            'Q5_Turnover': turnover,
            'Q5_Active_Return': active_ann_ret,
            
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
            print("Please run_analysis() first.")
            return
            
        # 1. Cumulative Returns
        plot_cumulative_returns(self.results['sorting']['quintile_returns'], self.results['sorting']['ls_returns'])
        
        # 2. IC Series
        plot_ic_series(self.results['ic']['IC_Series'])
        
        # 3. Quantile Bar
        plot_quantile_bar(self.results['sorting']['quintile_returns'])
