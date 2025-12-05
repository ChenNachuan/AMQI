import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class RSI(BaseFactor):
    """
    Relative Strength Index (RSI) Factor.
    
    RSI 是一个动量振荡器,衡量价格变动的速度和幅度。
    通过计算一定周期内平均涨幅与平均跌幅的比值,得到相对强度,
    再将其标准化到 0-100 区间。
    
    计算公式:
    1. 计算价格变动: change = close(t) - close(t-1)
    2. 分离涨跌: gain = max(change, 0), loss = max(-change, 0)
    3. 使用 Wilder's 平滑法计算平均涨跌幅:
       - 初始: avg_gain = mean(gain[1:period]), avg_loss = mean(loss[1:period])
       - 迭代: avg_gain = (avg_gain_prev * (period-1) + gain) / period
    4. 计算 RS = avg_gain / avg_loss
    5. 计算 RSI = 100 - (100 / (1 + RS))
    
    信号解读:
    - RSI > 70: 超买状态,价格可能被高估,存在回调风险
    - RSI < 30: 超卖状态,价格可能被低估,存在反弹机会
    - RSI ≈ 50: 中性状态
    """
    
    def __init__(self, period=14):
        """
        初始化 RSI 因子
        
        Args:
            period: RSI 计算周期,默认 14 天
        """
        self.period = period
    
    @property
    def name(self) -> str:
        return f"RSI_{self.period}"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 RSI 因子 (使用标准 Wilder's 平滑方法)
        
        Args:
            df: DataFrame,必须包含 'ts_code', 'trade_date', 'close' 列
            
        Returns:
            DataFrame,包含 RSI 因子值,索引为 ['trade_date', 'ts_code']
        """
        self.check_dependencies(df)
        
        # 按股票代码和日期排序 (与 turnover.py 保持一致,不使用 .copy())
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 计算价格变动
        df['price_change'] = df.groupby('ts_code')['close'].diff()
        
        # 分离涨幅和跌幅
        df['gain'] = df['price_change'].apply(lambda x: x if x > 0 else 0)
        df['loss'] = df['price_change'].apply(lambda x: -x if x < 0 else 0)
        
        # 使用 Wilder's 平滑方法计算平均涨跌幅
        # 注意: 这里使用 ewm 的等价方式,alpha = 1/period 对应 Wilder's 平滑
        # adjust=False 确保使用递归形式: y_t = alpha * x_t + (1-alpha) * y_{t-1}
        df['avg_gain'] = df.groupby('ts_code')['gain'].transform(
            lambda x: x.ewm(alpha=1/self.period, min_periods=self.period, adjust=False).mean()
        )
        df['avg_loss'] = df.groupby('ts_code')['loss'].transform(
            lambda x: x.ewm(alpha=1/self.period, min_periods=self.period, adjust=False).mean()
        )
        
        # 计算相对强度 RS = Average Gain / Average Loss
        # 处理除零情况: 当 avg_loss 为 0 时,RS 视为无穷大,RSI = 100
        df['RS'] = np.where(df['avg_loss'] != 0, df['avg_gain'] / df['avg_loss'], np.inf)
        
        # 计算 RSI = 100 - (100 / (1 + RS))
        rsi = np.where(
            np.isinf(df['RS']), 
            100.0, 
            100 - (100 / (1 + df['RS']))
        )
        
        # 构造结果 DataFrame (与 turnover.py 保持一致的格式)
        result = pd.DataFrame({
            self.name: rsi,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result