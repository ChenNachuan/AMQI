import pandas as pd
import numpy as np
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path.cwd().parent.parent
sys.path.insert(0, str(project_root))

from factor_library.base_factor import BaseFactor


class TurnoverVolatilityCoefficient(BaseFactor):
    """
    Turnover Volatility Coefficient Factor (TVC).
    
    换手率波动率系数 = std(T_t, T_{t-1}, ..., T_{t-K+1}) / mean(T_t, T_{t-1}, ..., T_{t-K+1})
    
    其中:
    - T_t: t日的日度换手率 = Volume_t / SharesOutstanding_t
    - K: 计算换手率波动率系数的时间窗口长度(单位:月), 本因子中取K=3
    - std: 最近K个月日度换手率序列的标准差
    - mean: 最近K个月日度换手率序列的平均值
    
    较高的换手率波动率系数意味着股票流动性风险较高,交易活跃度预期不稳定。
    """
    
    @property
    def name(self) -> str:
        return "TVC"
        
    @property
    def required_fields(self) -> list:
        return ['turnover_rate']
        
    def calculate(self, df: pd.DataFrame, window_months: int = 3) -> pd.DataFrame:
        """
        Calculate Turnover Volatility Coefficient.
        
        Args:
            df: DataFrame with 'turnover_rate' (日度换手率).
            window_months: 时间窗口长度(月), 默认为3个月.
            
        Returns:
            DataFrame with 'TVC' column.
        """
        self.check_dependencies(df)
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 将月数转换为交易日数 (约21个交易日/月)
        window = window_months * 21
        
        # 计算滚动标准差
        turnover_std = df.groupby('ts_code')['turnover_rate'].rolling(
            window, min_periods=window//2
        ).std().reset_index(0, drop=True)
        
        # 计算滚动均值
        turnover_mean = df.groupby('ts_code')['turnover_rate'].rolling(
            window, min_periods=window//2
        ).mean().reset_index(0, drop=True)
        
        # 计算换手率波动率系数 = std / mean
        # 避免除以0的情况
        tvc = turnover_std / turnover_mean.replace(0, np.nan)
        
        # 构建结果DataFrame
        result = pd.DataFrame({
            self.name: tvc,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result