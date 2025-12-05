import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class DownsideRiskBeta(BaseFactor):
    """
    下行风险Beta因子（修正版）
    衡量市场下跌时个股收益率对市场收益率的敏感度
    
    公式: β⁻ = Cov(r_i, r_m | r_m < μ_m) / Var(r_m | r_m < μ_m)
    """
    
    def __init__(self, window: int = 12, min_periods: int = 6, market_code: str = '000300.SH'):
        """
        初始化下行风险Beta因子
        
        参数:
            window: 滚动窗口大小（月数），默认12个月
            min_periods: 计算所需的最小月份数量，默认6个月
            market_code: 市场指数代码（默认：000300.SH 沪深300指数）
        """
        self.window = window
        self.min_periods = min_periods
        self.market_code = market_code
    
    @property
    def name(self) -> str:
        return "DownsideRiskBeta"
        
    @property
    def required_fields(self) -> list:
        # 使用ret字段（收益率），这是数据集中实际存在的字段
        return ['ret']
        
    def calculate(self, df: pd.DataFrame, market_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        计算下行风险Beta
        
        参数:
            df: 包含'ts_code'、'trade_date'和'ret'列的DataFrame（日度数据）
            market_df: 市场指数数据的DataFrame。如果为None，将尝试从df中提取
            
        返回:
            包含'DownsideRiskBeta'列的DataFrame，以[trade_date, ts_code]为索引
        """
        self.check_dependencies(df)
        
        # 排序数据
        df = df.sort_values(['ts_code', 'trade_date']).copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 提取市场数据
        if market_df is None:
            if self.market_code in df['ts_code'].values:
                market_df = df[df['ts_code'] == self.market_code].copy()
            else:
                raise ValueError(f"数据中未找到市场指数 {self.market_code}。")
        
        # 确保市场数据有ret字段
        if 'ret' not in market_df.columns:
            raise ValueError("市场数据必须包含'ret'列。")
        
        # 添加月份标识
        df['year_month'] = df['trade_date'].dt.to_period('M')
        market_df['year_month'] = pd.to_datetime(market_df['trade_date']).dt.to_period('M')
        
        # 计算月度收益率（从日收益率复合）
        # 月度收益率 = (1 + r1) * (1 + r2) * ... * (1 + rn) - 1
        
        # 股票月度收益率
        monthly_stock_returns = df.groupby(['ts_code', 'year_month'])['ret'].apply(
            lambda x: (1 + x).prod() - 1
        ).reset_index()
        monthly_stock_returns.columns = ['ts_code', 'year_month', 'monthly_ret']
        
        # 市场月度收益率
        monthly_market_returns = market_df.groupby('year_month')['ret'].apply(
            lambda x: (1 + x).prod() - 1
        ).reset_index()
        monthly_market_returns.columns = ['year_month', 'mkt_monthly_ret']
        
        # 合并月度数据
        monthly_data = pd.merge(
            monthly_stock_returns,
            monthly_market_returns,
            on='year_month',
            how='inner'
        )
        
        # 按股票分组计算下行Beta
        results = []
        
        for ts_code, group in monthly_data.groupby('ts_code'):
            if ts_code == self.market_code:
                continue  # 跳过市场指数本身
            
            group = group.sort_values('year_month').reset_index(drop=True)
            
            # 滚动窗口计算
            beta_values = []
            months = []
            
            for i in range(len(group)):
                # 获取窗口数据
                start_idx = max(0, i - self.window + 1)
                window_data = group.iloc[start_idx:i+1]
                
                if len(window_data) < self.min_periods:
                    beta_values.append(np.nan)
                    months.append(group.loc[i, 'year_month'])
                    continue
                
                # 计算市场月度收益率的均值 μ_m（注意：这里用的是月度收益率的均值）
                mu_m = window_data['mkt_monthly_ret'].mean()
                
                # 筛选下行月份：市场月度收益率 < 市场月度收益率均值
                downside_data = window_data[window_data['mkt_monthly_ret'] < mu_m]
                
                # 至少需要2个下行月份才能计算协方差
                if len(downside_data) < 2:
                    beta_values.append(np.nan)
                    months.append(group.loc[i, 'year_month'])
                    continue
                
                # 计算协方差和方差
                stock_downside_ret = downside_data['monthly_ret'].values
                market_downside_ret = downside_data['mkt_monthly_ret'].values
                
                # 协方差矩阵
                cov_matrix = np.cov(stock_downside_ret, market_downside_ret)
                cov = cov_matrix[0, 1]
                
                # 市场方差
                var = np.var(market_downside_ret, ddof=1)
                
                # 计算Beta
                if var > 1e-10:  # 避免除以接近0的数
                    beta = cov / var
                else:
                    beta = np.nan
                
                beta_values.append(beta)
                months.append(group.loc[i, 'year_month'])
            
            # 创建结果DataFrame
            stock_result = pd.DataFrame({
                'year_month': months,
                'ts_code': ts_code,
                self.name: beta_values
            })
            
            results.append(stock_result)
        
        # 合并所有结果
        if not results:
            return pd.DataFrame(columns=['year_month', 'ts_code', self.name])
        
        result = pd.concat(results, ignore_index=True)
        
        # 将月度结果扩展到日度数据
        # 每个月的beta值应用到该月的所有交易日
        df_expanded = df[df['ts_code'] != self.market_code][['ts_code', 'trade_date', 'year_month']].copy()
        result_expanded = pd.merge(
            df_expanded,
            result,
            on=['ts_code', 'year_month'],
            how='left'
        )
        
        # 设置索引并排序
        result_final = result_expanded[['trade_date', 'ts_code', self.name]].copy()
        result_final = result_final.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result_final