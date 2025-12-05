import pandas as pd
import numpy as np
import sys
sys.path.append('..')
from .base_factor import BaseFactor

class DailyTurnoverRate(BaseFactor):
    """
    日均换手率因子 (Daily Turnover Rate).
    计算公式: TurnoverRate = PeriodVolume / TotalShares
    
    该因子评估市场活跃度和股票流动性，高换手率意味着市场对该股票的兴趣较高，
    交易活跃，买卖盘较为充足。可以反映市场情绪的波动或主力资金的频繁操作。
    
    注意：数据源(Tushare daily_basic接口)已提供turnover_rate字段，
    该字段计算公式为：成交量(手)/流通股本(万股)，直接表示当日换手率(%)
    """
    
    @property
    def name(self) -> str:
        return "DailyTurnoverRate"
        
    @property
    def required_fields(self) -> list:
        # 使用Tushare API提供的turnover_rate字段
        # turnover_rate: 换手率(%)，计算公式 = 成交量(手)/流通股本(万股)
        return ['turnover_rate']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算日均换手率。
        
        Args:
            df: DataFrame，需包含以下字段:
                - turnover_rate: 日换手率(%)，来自Tushare daily_basic接口
                - trade_date: 交易日期
                - ts_code: 股票代码
            
        Returns:
            DataFrame，包含 'DailyTurnoverRate' 列，表示日换手率(%)
            
        经济意义：
            - 直接使用当日换手率，反映当日市场活跃度
            - 高换手率(>5%)通常表示交易活跃、流动性好
            - 低换手率(<1%)可能表示流动性不足或股东结构稳定
            - 可用于筛选活跃股票、评估市场情绪、计算流动性风险
        """
        self.check_dependencies(df)
        
        # 按股票代码和交易日期排序
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 直接使用turnover_rate字段作为日均换手率
        # 该字段已经是标准化的换手率指标(%)
        daily_turnover = df['turnover_rate']
        
        # 构建结果DataFrame
        result = pd.DataFrame({
            self.name: daily_turnover,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        # 设置多重索引并排序
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result