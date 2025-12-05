import pandas as pd
import numpy as np
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path('..').parent.resolve()
sys.path.insert(0, str(project_root))

from factor_library.base_factor import BaseFactor


class CVILLIQ(BaseFactor):
    """
    非流动性变异系数 (Coefficient of Variation of Illiquidity).
    
    衡量股票非流动性指标在一段时间内的波动程度。
    较高的CVILLIQ值意味着该股票的非流动性波动较大,投资者可能会要求更高的风险溢价。
    
    计算公式:
    1. ILLIQ_{i,t} = |r_{i,t}| / Amount_{i,t}
    2. CVILLIQ_i = σ(ILLIQ_i) / mean(ILLIQ_i)
    
    其中:
    - r_{i,t}: 股票 i 在 t 时刻的收益率(小数形式)
    - Amount_{i,t}: 股票 i 在 t 时刻的交易金额(转换为百万元,与学术文献保持一致)
    - σ(ILLIQ_i): ILLIQ 在时间窗口内的标准差
    - mean(ILLIQ_i): ILLIQ 在时间窗口内的平均值
    - 时间窗口: 过去20个交易日
    
    注意:
    - 原始 amount 字段单位为千元,需转换为百万元(除以1000)
    - ILLIQ 的单位为: 1/(百万元),表示每百万元交易金额对应的价格冲击
    - CVILLIQ 为无量纲指标(变异系数)
    """
    
    @property
    def name(self) -> str:
        return "CVILLIQ"
        
    @property
    def required_fields(self) -> list:
        return ['pct_chg', 'amount']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算非流动性变异系数。
        
        Args:
            df: DataFrame with 'pct_chg' (涨跌幅, %) and 'amount' (成交额, 千元).
            
        Returns:
            DataFrame with 'CVILLIQ' column.
        """
        self.check_dependencies(df)
        
        df = df.sort_values(['ts_code', 'trade_date']).copy()
        
        # 计算收益率 (将百分比转换为小数)
        df['return'] = df['pct_chg'] / 100.0
        
        # 将交易金额从千元转换为百万元 (与学术文献保持一致)
        # amount单位: 千元 -> 百万元需要除以1000
        df['amount_million'] = df['amount'] / 1000.0
        
        # 计算非流动性指标 ILLIQ = |return| / amount_million
        # 避免除以零或过小的交易金额
        # 设置最小阈值: 0.001百万元 = 1000元
        min_amount = 0.001  # 百万元
        df['amount_million'] = df['amount_million'].clip(lower=min_amount)
        
        df['illiq'] = np.abs(df['return']) / df['amount_million']
        
        # 设置时间窗口
        window = 20  # 过去20个交易日
        
        # 按股票分组,计算滚动标准差和滚动均值
        grouped = df.groupby('ts_code')['illiq']
        
        # 滚动标准差
        illiq_std = grouped.rolling(window, min_periods=window).std().reset_index(0, drop=True)
        
        # 滚动均值
        illiq_mean = grouped.rolling(window, min_periods=window).mean().reset_index(0, drop=True)
        
        # 计算变异系数 CV = std / mean
        # 当均值过小时(接近零),变异系数失去意义,设为 NaN
        min_mean_threshold = 1e-8
        cvilliq = np.where(
            illiq_mean > min_mean_threshold,
            illiq_std / illiq_mean,
            np.nan
        )
        
        # 构建结果 DataFrame
        result = pd.DataFrame({
            self.name: cvilliq,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result