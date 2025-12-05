
import pandas as pd
import numpy as np
from factor_library.base_factor import BaseFactor

class HighPrice52Week(BaseFactor):
    """
    52周最高价逼近度因子 (HP52W).
    当前收盘价与过去52周最高价的比值。
    """
    
    @property
    def name(self) -> str:
        return "HP52W"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算52周最高价逼近度。
        
        Args:
            df: DataFrame with 'close' column (后复权价格).
            
        Returns:
            DataFrame with 'HP52W' column.
        """
        self.check_dependencies(df)
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 52周 = 52 * 5 = 260个交易日（更精确）
        # 或使用 252 作为一年的交易日数（约定俗成）
        window = 260  # 52周的交易日数
        
        # 计算过去52周（包含当前）的最高价
        # rolling(window).max() 会计算包含当前在内的window个交易日的最大值
        max_52w = df.groupby('ts_code')['close'].rolling(window, min_periods=window).max().reset_index(0, drop=True)
        
        # 计算52周最高价逼近度 = 当前收盘价 / 过去52周最高价
        hp52w = df['close'] / max_52w
        
        # 构建结果DataFrame
        result = pd.DataFrame({
            self.name: hp52w,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
