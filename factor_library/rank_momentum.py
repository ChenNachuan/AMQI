import pandas as pd
import numpy as np
from factor_library.base_factor import BaseFactor

class RankMomentum(BaseFactor):
    """
    Rank Momentum Factor (RankMomentum).
    
    该因子通过以下步骤计算：
    1. 计算每日股票收益率并对其进行排名
    2. 对每日的排名进行标准化处理（均值为0，标准差为1）
    3. 对每个月的标准化排名得分取平均，得到月度的排名得分均值
    4. 计算过去N个月（偏移M个月）的月度排名得分均值的平均值
    
    参数：
        N: 考察时间窗口大小（月份数），默认为6
        M: 时间偏移量（月份数），默认为1
    """
    
    def __init__(self, N: int = 6, M: int = 1):
        """
        初始化排序动量因子
        
        Args:
            N: 考察时间窗口大小（月份数），默认为6
            M: 时间偏移量（月份数），默认为1
        """
        self.N = N
        self.M = M
    
    @property
    def name(self) -> str:
        return f"RankMomentum_{self.N}_{self.M}"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算排序动量因子
        
        Args:
            df: DataFrame包含 'close', 'trade_date', 'ts_code' 列
            
        Returns:
            DataFrame with 'RankMomentum' column, indexed by [trade_date, ts_code]
        """
        self.check_dependencies(df)
        
        # 确保数据按日期和股票代码排序
        df = df.sort_values(['trade_date', 'ts_code']).copy()
        
        # 确保 trade_date 是 datetime 格式
        if not pd.api.types.is_datetime64_any_dtype(df['trade_date']):
            df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str))
        
        # 步骤1: 计算每日收益率 R_i,d = (P_i,d - P_i,d-1) / P_i,d-1
        df['return'] = df.groupby('ts_code')['close'].pct_change()
        
        # 步骤2: 计算每日排名并标准化
        # rank_{i,d} = (y(R_{i,d}) - (N_d+1)/2) / sqrt((N_d+1)(N_d-1)/12)
        def standardize_rank(group):
            """对每日的收益率进行排名并标准化"""
            # 排名（升序，从1开始）
            ranks = group.rank(method='average')
            N_d = group.count()  # 有效股票数量
            
            # 标准化
            if N_d > 1:
                mean_rank = (N_d + 1) / 2
                std_rank = np.sqrt((N_d + 1) * (N_d - 1) / 12)
                standardized = (ranks - mean_rank) / std_rank
            else:
                standardized = pd.Series(0, index=ranks.index)
            
            return standardized
        
        df['rank_std'] = df.groupby('trade_date')['return'].transform(standardize_rank)
        
        # 步骤3: 计算月度排名标准化得分均值
        # 添加年月列用于分组
        df['year_month'] = df['trade_date'].dt.to_period('M')
        
        monthly_rank = df.groupby(['ts_code', 'year_month'])['rank_std'].mean().reset_index()
        monthly_rank.rename(columns={'rank_std': 'monthly_rank_mean'}, inplace=True)
        
        # 步骤4: 计算过去N个月（偏移M个月）的月度排名得分均值的平均值
        # 将月度数据按股票和时间排序
        monthly_rank = monthly_rank.sort_values(['ts_code', 'year_month'])
        
        # 修正后的计算逻辑：
        # 对于时刻t，取[t-N-M+1, ..., t-M]的均值
        # 即：先计算rolling mean，再shift M个月
        def calculate_rank_momentum(group):
            """
            计算每个股票的排序动量因子
            公式: RankMomentum_{i,t}(N,M) = (1/N) * sum_{m=t-N-M+1}^{t-M} rank_{i,m}
            实现: 先rolling(N).mean()得到过去N个月均值，再shift(M)偏移到t-M时刻
            """
            rank_momentum = group['monthly_rank_mean'].rolling(
                window=self.N, 
                min_periods=self.N
            ).mean().shift(self.M)
            return rank_momentum
        
        monthly_rank['RankMomentum'] = monthly_rank.groupby('ts_code').apply(
            calculate_rank_momentum
        ).reset_index(level=0, drop=True)
        
        # 将月度因子值扩展到每日
        # 合并回原始数据框
        df = df.merge(
            monthly_rank[['ts_code', 'year_month', 'RankMomentum']],
            on=['ts_code', 'year_month'],
            how='left'
        )
        
        # 准备返回结果
        result = pd.DataFrame({
            self.name: df['RankMomentum'],
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        # 设置索引并排序
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result