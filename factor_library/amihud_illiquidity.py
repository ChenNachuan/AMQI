import pandas as pd
import numpy as np
from factor_library.base_factor import BaseFactor

class AmihudIlliquidity(BaseFactor):
    """
    Amihud 非流动性因子 (ILLIQ)。
    衡量交易量对价格的影响，捕捉市场非流动性。
    数值越高表示流动性越差，交易成本越高。
    """
    
    @property
    def name(self) -> str:
        return "ILLIQ"
        
    @property
    def required_fields(self) -> list:
        return ['close', 'pre_close', 'amount']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算带标准化的 Amihud 非流动性指标。
        
        公式:
        ILLIQ_{i,t} = (1/D_t) * Σ(|R_{i,d}| / VOLD_{i,d})
        
        其中：
        - R_{i,d}: 股票 i 在第 d 日的收益率
        - VOLD_{i,d}: 股票 i 在第 d 日的成交额（单位：百万元）
        - D_t: 有效交易日总数（最少要求15天）
        
        标准化处理：
        1. 成交额缩放到百万元单位（÷ 1,000,000），提高可解释性
        2. 对最终因子值进行横截面标准化（z-score）
        
        参数:
            df: 包含 'close', 'pre_close', 'amount' 的 DataFrame。
            
        返回:
            包含标准化后 'ILLIQ' 列的 DataFrame。
        """
        self.check_dependencies(df)
        
        df = df.sort_values(['ts_code', 'trade_date']).copy()
        
        # 计算日收益率绝对值 |R_{i,d}|
        df['daily_return'] = (df['close'] / df['pre_close'] - 1).abs()
        
        # 将成交额标准化为百万元单位，提高可读性
        # 使指标更易解释："每百万元交易引起的价格变动百分比"
        df['amount_millions'] = df['amount'] / 1_000_000
        
        # 计算日度非流动性比率: |R_{i,d}| / VOLD_{i,d}
        # 过滤极小或零成交额，避免极端值
        min_amount = 0.01  # 至少1万元
        df['amount_millions'] = df['amount_millions'].clip(lower=min_amount)
        
        df['daily_illiq'] = df['daily_return'] / df['amount_millions']
        
        # 替换无穷值和缺失值
        df['daily_illiq'] = df['daily_illiq'].replace([np.inf, -np.inf], np.nan)
        
        window = 20  # 约1个月交易日
        min_periods = 15  # 最少要求的交易日数
        
        # 计算滚动平均的日度非流动性
        illiq = df.groupby('ts_code')['daily_illiq'].rolling(
            window=window, 
            min_periods=min_periods
        ).mean().reset_index(0, drop=True)
        
        # 创建结果数据框
        result = pd.DataFrame({
            'ILLIQ_raw': illiq,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        # 横截面标准化（z-score 归一化）
        # 在每个日期内标准化，使均值=0，标准差=1
        def standardize_cross_section(group):
            mean = group['ILLIQ_raw'].mean()
            std = group['ILLIQ_raw'].std()
            if std > 0:
                group[self.name] = (group['ILLIQ_raw'] - mean) / std
            else:
                group[self.name] = 0
            return group
        
        result = result.groupby('trade_date').apply(standardize_cross_section)
        
        # 仅保留最终标准化后的列
        result = result[[self.name, 'trade_date', 'ts_code']]
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result