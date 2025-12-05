import pandas as pd
import numpy as np
from factor_library.base_factor import BaseFactor

class HistoricalVolatility(BaseFactor):
    """
    Historical Volatility Factor (HVOL).
    Standard deviation of daily returns over the past K trading days.
    历史波动率因子:过去K个交易日日度收益率的标准差
    """
    
    @property
    def name(self) -> str:
        return "HVOL"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Historical Volatility.
        
        Args:
            df: DataFrame with 'close' price and columns ['ts_code', 'trade_date', 'close'].
            
        Returns:
            DataFrame with 'HVOL' column.
        """
        self.check_dependencies(df)
        
        # 创建副本避免修改原始数据
        df = df.copy()
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 时间窗口长度K=20个交易日
        window = 20
        
        # 计算日度收益率 r_i = (close_i - close_{i-1}) / close_{i-1}
        daily_return = df.groupby('ts_code')['close'].pct_change()
        
        # 计算过去K个交易日收益率的标准差
        # 使用pandas rolling std,ddof=1表示样本标准差(除以K-1)
        hvol = daily_return.groupby(df['ts_code']).rolling(window, min_periods=window).std().reset_index(0, drop=True)
        
        # 构建结果DataFrame
        result = pd.DataFrame({
            self.name: hvol,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result