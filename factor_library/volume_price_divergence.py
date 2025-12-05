import pandas as pd
import numpy as np
import sys
sys.path.append('c:/Users/30515/Desktop/大作业/AMQI-main')
from factor_library.base_factor import BaseFactor

class VolumeVWAPDivergence(BaseFactor):
    """
    成交量价背离度因子（月频）
    
    计算月度VWAP变化率与月度成交量变化率在指定时间窗口内的皮尔逊相关系数。
    正值表示价量同向变化，负值表示价量背离。
    
    因子说明：
    - 将日频数据聚合为月频数据
    - 月度VWAP：当月的成交量加权平均价格
    - 月度成交量：当月总成交量
    - VWAP_pct：月度VWAP的变化率，反映月度价格变动方向
    - VOL_pct：月度成交量的变化率，反映月度成交量变动方向
    - 在滚动窗口内计算两个变化率序列的相关系数（窗口单位：月）
    - 正相关：价涨量增或价跌量减，价量配合，趋势健康
    - 负相关：价涨量减或价跌量增，价量背离，趋势可疑（如"无量上涨"）
    """
    
    def __init__(self, window: int = 12):
        """
        初始化因子
        
        Args:
            window: 计算相关系数的时间窗口（月数），默认为12个月
        """
        self.window = window
    
    @property
    def name(self) -> str:
        return f"VolumeVWAPDivergence_{self.window}m"
        
    @property
    def required_fields(self) -> list:
        return ['vol', 'amount']  # amount(千元), vol(手)
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算成交量价背离度因子
        
        Args:
            df: 包含close, vol, amount字段的DataFrame
            
        Returns:
            包含因子值的DataFrame，索引为[trade_date, ts_code]
        """
        self.check_dependencies(df)
        
        # 确保数据排序
        df = df.sort_values(['ts_code', 'trade_date']).copy()
        
        # 计算VWAP (每日成交量加权平均价格)
        # 注意单位转换：amount是千元，vol是手(100股)
        # VWAP(元/股) = amount(千元) * 1000 / (vol(手) * 100)
        #              = amount * 10 / vol
        df['vwap'] = (df['amount'] * 10) / (df['vol'] + 1e-10)  # 避免除零
        
        # 计算VWAP的变化率（日变化率）
        # 按股票分组计算，避免跨股票计算
        df['vwap_pct'] = df.groupby('ts_code')['vwap'].pct_change()
        
        # 计算成交量的变化率
        df['vol_pct'] = df.groupby('ts_code')['vol'].pct_change()
        
        # 使用向量化方式计算滚动相关系数
        # 按股票分组，计算VWAP变化率与成交量变化率的滚动相关系数
        def calc_rolling_corr(group):
            """计算VWAP变化率与成交量变化率的滚动相关系数"""
            vwap_pct_series = group['vwap_pct']
            vol_pct_series = group['vol_pct']
            
            # 使用pandas的rolling.corr计算滚动窗口相关系数
            # 计算两个变化率序列的相关性，反映价量变动的配合程度
            corr = vwap_pct_series.rolling(window=self.window).corr(vol_pct_series)
            
            return corr
        
        # 按股票分组计算
        factor_values = df.groupby('ts_code', group_keys=False).apply(calc_rolling_corr)
        
        # 准备结果
        result = pd.DataFrame({
            self.name: factor_values.values,
            'trade_date': df['trade_date'].values,
            'ts_code': df['ts_code'].values
        })
        
        # 设置索引
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result