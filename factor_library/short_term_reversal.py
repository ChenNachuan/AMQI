import pandas as pd
import numpy as np
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path.cwd().parent.parent
sys.path.insert(0, str(project_root))

from factor_library.base_factor import BaseFactor


class ShortTermReversal(BaseFactor):
    """
    Short-Term Reversal Factor (STR).
    计算股票在指定时间窗口内的收益率: R_t = (P_t - P_{t-n}) / P_{t-n}
    并取负值以反映反转效应。
    
    短期反转效应基于市场对短期信息的过度反应，认为近期表现不佳的股票
    可能会反转上涨，而近期表现良好的股票可能会回调。
    
    因子值解释：
    - 正值：该股票近期下跌（负收益率取负），预期反转上涨
    - 负值：该股票近期上涨（正收益率取负），预期反转下跌
    """
    
    def __init__(self, period: int = 20):
        """
        初始化短期反转因子
        
        Args:
            period: 计算收益率的时间窗口（交易日），默认20天约为1个月
                   注意：这是交易日数量，不是自然日
        """
        self.period = period
    
    @property
    def name(self) -> str:
        return f"STR_{self.period}"
        
    @property
    def required_fields(self) -> list:
        # 需要收盘价、交易日期和股票代码
        return ['close', 'trade_date', 'ts_code']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算短期反转因子
        
        Args:
            df: DataFrame with 'close', 'trade_date', 'ts_code'
            
        Returns:
            DataFrame with 'STR_N' column (N为period参数)
        """
        self.check_dependencies(df)
        
        # 创建副本避免修改原始数据
        df = df.copy()
        
        # 按股票代码和交易日期排序
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 计算period期的收益率: (P_t - P_{t-period}) / P_{t-period}
        # 使用shift获取period天前的价格
        df['prev_close'] = df.groupby('ts_code')['close'].shift(self.period)
        
        # 计算收益率
        returns = (df['close'] - df['prev_close']) / df['prev_close']
        
        # 短期反转策略：收益率取负值（做多表现差的，做空表现好的）
        # 这样：负收益率（下跌）→ 正因子值（买入信号）
        #      正收益率（上涨）→ 负因子值（卖出信号）
        str_value = -returns
        
        # 处理异常值：过滤掉极端收益率（可能是数据错误）
        # 例如：日收益率超过±50%可能是异常值
        str_value = str_value.where(
            (returns.abs() <= 0.5) | returns.isna(),
            np.nan
        )
        
        # 构建结果
        result = pd.DataFrame({
            self.name: str_value,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result