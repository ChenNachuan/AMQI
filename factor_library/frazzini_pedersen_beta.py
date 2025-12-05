import pandas as pd
import numpy as np
from typing import Optional

class AdjustedBetaFP:
    """
    Frazzini-Pedersen 调整贝塔因子
    
    公式: β_FP = ρ_im × (σ_i / σ_m)
    
    其中:
    - σ_i: 股票i过去K个月对数收益率的标准差
    - σ_m: 市场基准过去K个月对数收益率的标准差
    - ρ_im: 股票i与市场基准过去Y年的日度收益率相关系数（使用3天重叠收益率）
    - K: 12个月（至少120个有效日度收益率）
    - Y: 5年（至少750个有效日度收益率）
    """
    
    def __init__(self, 
                 volatility_window_months: int = 12,
                 correlation_window_years: int = 5,
                 market_benchmark: str = '000300.SH'):  # 沪深300作为市场基准
        """
        初始化调整贝塔FP因子
        
        参数:
            volatility_window_months: 波动率计算的窗口长度（K个月）
            correlation_window_years: 相关系数计算的窗口长度（Y年）
            market_benchmark: 市场基准指数代码（默认：沪深300）
        """
        self.volatility_window_months = volatility_window_months
        self.correlation_window_years = correlation_window_years
        self.market_benchmark = market_benchmark
        
        # 转换为交易日数（近似值）
        self.volatility_window_days = volatility_window_months * 20
        self.correlation_window_days = correlation_window_years * 252
        
    @property
    def name(self) -> str:
        return "AdjustedBeta_FP"
        
    @property
    def required_fields(self) -> list:
        """输入数据框中需要的列"""
        return ['ts_code', 'trade_date', 'close', 'pct_chg']
    
    def _calculate_log_returns(self, df: pd.DataFrame) -> pd.Series:
        """
        从百分比变化计算对数收益率
        
        参数:
            df: 包含'pct_chg'列的数据框
            
        返回:
            对数收益率序列
        """
        # 将pct_chg转换为对数收益率: log(1 + r)
        return np.log(1 + df['pct_chg'] / 100)
    
    def _calculate_overlapping_returns(self, log_returns: pd.Series, window: int = 3) -> pd.Series:
        """
        计算用于相关系数计算的重叠收益率
        
        公式: r_it = (1/3) × Σ(k=0 to 2) log(1 + R_{t+k})
        
        参数:
            log_returns: 对数收益率序列
            window: 重叠窗口大小（默认：3天）
            
        返回:
            重叠收益率序列
        """
        # 使用滚动求和并除以窗口大小
        overlapping = log_returns.rolling(window=window, min_periods=window).sum() / window
        return overlapping
    
    def _calculate_volatility(self, log_returns: pd.Series, window: int) -> pd.Series:
        """
        计算对数收益率的滚动标准差
        
        参数:
            log_returns: 对数收益率序列
            window: 滚动窗口大小（天数）
            
        返回:
            滚动波动率序列
        """
        return log_returns.rolling(window=window, min_periods=int(window * 0.8)).std()
    
    def _calculate_correlation(self, 
                               stock_returns: pd.Series, 
                               market_returns: pd.Series, 
                               window: int) -> pd.Series:
        """
        计算股票与市场收益率的滚动相关系数
        
        参数:
            stock_returns: 股票重叠收益率序列
            market_returns: 市场重叠收益率序列
            window: 滚动窗口大小（天数）
            
        返回:
            滚动相关系数序列
        """
        # 对齐两个序列
        df = pd.DataFrame({
            'stock': stock_returns,
            'market': market_returns
        })
        
        # 计算滚动相关系数
        correlation = df['stock'].rolling(window=window, min_periods=int(window * 0.8)).corr(df['market'])
        return correlation
    
    def calculate(self, 
                  stock_df: pd.DataFrame, 
                  market_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算Frazzini-Pedersen调整贝塔
        
        参数:
            stock_df: 包含股票数据的数据框，需要包含 ['ts_code', 'trade_date', 'close', 'pct_chg']
            market_df: 包含市场基准数据的数据框，需要包含 ['trade_date', 'close', 'pct_chg']
                      注意：如果market_df中包含ts_code列，会被自动忽略
            
        返回:
            包含调整贝塔值的数据框，索引为 [trade_date, ts_code]
        """
        # 检查必需字段
        for field in self.required_fields:
            if field not in stock_df.columns:
                raise ValueError(f"缺少必需列: {field}")
        
        # 检查市场数据必需字段
        market_required = ['trade_date', 'pct_chg']
        for field in market_required:
            if field not in market_df.columns:
                raise ValueError(f"市场数据缺少必需列: {field}")
        
        # 排序数据
        stock_df = stock_df.sort_values(['ts_code', 'trade_date']).copy()
        market_df = market_df.sort_values('trade_date').copy()
        market_df['overlapping_return'] = market_df['log_return'].rolling(window=3, min_periods=3).sum() / 3
        
        # 将市场数据与股票数据合并
        stock_df = stock_df.merge(
            market_df[['trade_date', 'log_return', 'overlapping_return']].rename(
                columns={
                    'log_return': 'market_log_return',
                    'overlapping_return': 'market_overlapping_return'
                }
            ),
            on='trade_date',
            how='left'
        )
        
        results = []
        
        # 对每只股票进行计算
        for ts_code, group in stock_df.groupby('ts_code'):
            group = group.sort_values('trade_date').copy()
            
            # 1. 计算 σ_i: 股票过去K个月的波动率
            sigma_i = self._calculate_volatility(
                group['log_return'], 
                self.volatility_window_days
            )
            
            # 2. 计算 σ_m: 市场过去K个月的波动率
            sigma_m = self._calculate_volatility(
                group['market_log_return'], 
                self.volatility_window_days
            )
            
            # 3. 计算 ρ_im: 过去Y年使用重叠收益率的相关系数
            rho_im = self._calculate_correlation(
                group['overlapping_return'],
                group['market_overlapping_return'],
                self.correlation_window_days
            )
            
            # 4. 计算 β_FP = ρ_im × (σ_i / σ_m)
            beta_fp = rho_im * (sigma_i / sigma_m)
            
            # 创建结果数据框
            result = pd.DataFrame({
                self.name: beta_fp,
                'trade_date': group['trade_date'],
                'ts_code': ts_code
            })
            
            results.append(result)
        
        # 合并所有结果
        final_result = pd.concat(results, ignore_index=True)
        
        # 设置索引并排序
        final_result = final_result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return final_result


# 便捷函数：使用等权市场收益率
def calculate_adjusted_beta_fp_equal_weight(stock_data: pd.DataFrame,
                                             volatility_months: int = 12,
                                             correlation_years: int = 5) -> pd.DataFrame:
    """
    便捷函数：使用等权市场收益率计算Frazzini-Pedersen调整贝塔因子
    
    参数:
        stock_data: 股票数据，需包含 ['ts_code', 'trade_date', 'close', 'pct_chg']
        volatility_months: 波动率计算窗口（月）
        correlation_years: 相关系数计算窗口（年）
        
    返回:
        包含调整贝塔值的数据框
    """
    # 计算等权市场收益率（所有股票的平均收益率）
    market_data = stock_data.groupby('trade_date').agg({
        'pct_chg': 'mean',
        'close': 'mean'
    }).reset_index()
    
    # 创建因子对象并计算
    factor = AdjustedBetaFP(
        volatility_window_months=volatility_months,
        correlation_window_years=correlation_years
    )
    
    return factor.calculate(stock_data, market_data)


# 便捷函数：使用指定指数作为市场基准
def calculate_adjusted_beta_fp_index(stock_data: pd.DataFrame,
                                      index_code: str = '000300.SH',
                                      volatility_months: int = 12,
                                      correlation_years: int = 5) -> pd.DataFrame:
    """
    便捷函数：使用指定指数作为市场基准计算Frazzini-Pedersen调整贝塔因子
    
    参数:
        stock_data: 股票数据，需包含 ['ts_code', 'trade_date', 'close', 'pct_chg']
        index_code: 指数代码（默认：000300.SH 沪深300）
        volatility_months: 波动率计算窗口（月）
        correlation_years: 相关系数计算窗口（年）
        
    返回:
        包含调整贝塔值的数据框
        
    注意:
        此函数需要stock_data中包含指数数据，或者单独加载指数数据
    """
    # 从股票数据中筛选出指数数据作为市场基准
    market_data = stock_data[stock_data['ts_code'] == index_code].copy()
    
    if len(market_data) == 0:
        raise ValueError(f"在数据中未找到指数 {index_code}，请检查数据或使用等权市场收益率方法")
    
    # 筛选出非指数的股票数据
    stock_data_filtered = stock_data[stock_data['ts_code'] != index_code].copy()
    
    # 创建因子对象并计算
    factor = AdjustedBetaFP(
        volatility_window_months=volatility_months,
        correlation_window_years=correlation_years,
        market_benchmark=index_code
    )
    
    return factor.calculate(stock_data_filtered, market_data)