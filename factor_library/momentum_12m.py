import pandas as pd
import numpy as np
from pathlib import Path
import sys

# 添加因子库路径
sys.path.append(str(Path.cwd().parent))
from .base_factor import BaseFactor


class Momentum12M(BaseFactor):
    """
    12个月累计收益率动量因子 (MOM12).
    计算过去12个月（不包含最近一个月）的累计收益率。
    
    公式: MOM(12) = sum(r_t) for t=1 to 12
    其中 r_t = (P_end - P_begin) / P_begin 为第t个月的月度收益率
    
    在日线数据上实现时，使用以下近似：
    - 1个月 ≈ 21个交易日
    - 12个月 ≈ 252个交易日
    - 13个月 ≈ 273个交易日
    - 计算公式: MOM12 = P_{t-21} / P_{t-273} - 1
    - 计算区间为 [t-273, t-21]，正好涵盖12个完整月份
    """
    
    @property
    def name(self) -> str:
        return "MOM12"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算12个月累计收益率动量因子（日度滚动计算）。
        
        逻辑:
        MOM12 = P_{t-21} / P_{t-273} - 1
        其中：
        - P_{t-21}: 21个交易日前的收盘价（约1个月前，排除最近一个月）
        - P_{t-273}: 273个交易日前的收盘价（约13个月前）
        - 该比率减1即为过去12个月（排除最近1个月）的累计收益率
        - 计算区间：[t-273, t-21]，正好是12个月（252个交易日）
           
        参数:
            df: 包含 'close' 字段的日线数据DataFrame
            
        返回:
            包含 'MOM12' 列的DataFrame，索引为 [trade_date, ts_code]
            返回每日的动量因子值
        """
        self.check_dependencies(df)
        
        # 确保数据按股票代码和交易日期排序
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 定义时间窗口（近似交易日数）
        lag_1m = 21    # 1个月约21个交易日（排除最近一个月）
        lag_12m = 252  # 12个月约252个交易日
        lag_13m = 273  # 13个月约273个交易日（21 + 252）
        
        # 计算 MOM12 = P_{t-21} / P_{t-273} - 1
        # 这样计算的是从 t-273天到t-21天之间的12个月累计收益率
        # 即：过去12个月（不包含最近1个月）的收益
        # 按 ts_code 分组，避免跨股票计算
        p_t_minus_1m = df.groupby('ts_code')['close'].shift(lag_1m)    # 1个月前的价格
        p_t_minus_13m = df.groupby('ts_code')['close'].shift(lag_13m)  # 13个月前的价格
        
        # 计算累计收益率（12个月的收益）
        mom12 = (p_t_minus_1m / p_t_minus_13m) - 1
        
        # 准备结果DataFrame
        result = pd.DataFrame({
            self.name: mom12,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        # 设置标准索引 [trade_date, ts_code] 并排序
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result