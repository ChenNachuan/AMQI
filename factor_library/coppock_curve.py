import pandas as pd
import numpy as np
import sys
sys.path.append('..')
from .base_factor import BaseFactor

class Coppock(BaseFactor):
    """
    科波克曲线因子 (Coppock Curve).
    
    计算公式:
    R(N1) = (CLOSE - CLOSE[N1]) / CLOSE[N1] * 100
    R(N2) = (CLOSE - CLOSE[N2]) / CLOSE[N2] * 100
    RC(N1, N2) = R(N1) + R(N2)
    COPPOCK(N1, N2, N3) = WMA(RC(N1, N2), N3)
    
    该指标通过计算不同周期的价格变化率的加权移动平均值来衡量市场动能，
    用于识别长期市场趋势。当科波克曲线从负值向上穿过零线时，通常被视为
    中期买入信号，预示着市场可能进入上升趋势。
    """
    
    def __init__(self, n1: int = 14, n2: int = 11, n3: int = 10):
        """
        初始化科波克曲线因子。
        
        Args:
            n1: 短期价格变动率计算周期，默认14
            n2: 中短期价格变动率计算周期，默认11
            n3: 加权移动平均的计算周期，默认10
        """
        super().__init__()
        self.n1 = n1
        self.n2 = n2
        self.n3 = n3
    
    @property
    def name(self) -> str:
        return "Coppock"
        
    @property
    def required_fields(self) -> list:
        return ['close']
    
    def _weighted_moving_average(self, series: pd.Series, window: int) -> pd.Series:
        """
        计算加权移动平均值 (WMA)。
        
        权重随时间递增，最近的数据点权重最大。
        权重公式: w[i] = i + 1, 其中 i = 0, 1, ..., window-1
        
        Args:
            series: 输入序列
            window: 窗口大小
            
        Returns:
            加权移动平均序列
        """
        # 创建权重数组: [1, 2, 3, ..., window]
        weights = np.arange(1, window + 1)
        
        def wma(x):
            if len(x) < window:
                return np.nan
            return np.sum(weights * x[-window:]) / np.sum(weights)
        
        # 使用rolling apply计算WMA
        return series.rolling(window=window, min_periods=window).apply(wma, raw=True)
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算科波克曲线。
        
        Args:
            df: DataFrame，需包含以下字段:
                - close: 收盘价
                - trade_date: 交易日期
                - ts_code: 股票代码
            
        Returns:
            DataFrame，包含 'Coppock' 列
            
        计算步骤:
            1. 计算N1周期价格变动率 R(N1)
            2. 计算N2周期价格变动率 R(N2)
            3. 计算组合动量指标 RC = R(N1) + R(N2)
            4. 计算RC的N3周期加权移动平均值
        """
        self.check_dependencies(df)
        
        # 按股票代码和交易日期排序
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 计算每个股票的科波克曲线
        def compute_coppock(group):
            """计算单只股票的科波克曲线"""
            close = group['close'].values
            n = len(close)
            
            # 步骤1: 计算N1周期价格变动率
            # R(N1) = (CLOSE - CLOSE[N1]) / CLOSE[N1] * 100
            r_n1 = np.full(n, np.nan)
            for i in range(self.n1, n):
                if close[i - self.n1] != 0:
                    r_n1[i] = (close[i] - close[i - self.n1]) / close[i - self.n1] * 100
            
            # 步骤2: 计算N2周期价格变动率
            # R(N2) = (CLOSE - CLOSE[N2]) / CLOSE[N2] * 100
            r_n2 = np.full(n, np.nan)
            for i in range(self.n2, n):
                if close[i - self.n2] != 0:
                    r_n2[i] = (close[i] - close[i - self.n2]) / close[i - self.n2] * 100
            
            # 步骤3: 计算组合动量指标
            # RC = R(N1) + R(N2)
            rc = r_n1 + r_n2
            
            # 步骤4: 计算加权移动平均
            # COPPOCK = WMA(RC, N3)
            # Convert rc to Series for rolling calculation
            coppock = self._weighted_moving_average(pd.Series(rc), self.n3)
            
            return coppock
        
        # 按股票分组计算
        coppock_values = df.groupby('ts_code', group_keys=False).apply(
            compute_coppock
        ).reset_index(drop=True)
        
        # 构建结果DataFrame
        result = pd.DataFrame({
            self.name: coppock_values,
            'trade_date': df['trade_date'].values,
            'ts_code': df['ts_code'].values
        })
        
        # 设置多重索引并排序
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result

if __name__ == "__main__":
    # 创建示例数据
    dates = pd.date_range('20240101', periods=50, freq='D')
    sample_data = pd.DataFrame({
        'ts_code': ['000001.SZ'] * 50,
        'trade_date': [d.strftime('%Y%m%d') for d in dates],
        'close': np.random.randn(50).cumsum() + 100  # 模拟价格随机游走
    })
    
    # 实例化因子（使用默认参数）
    factor = Coppock(n1=14, n2=11, n3=10)
    
    # 计算因子
    result = factor.calculate(sample_data)
    
    print("科波克曲线计算结果:")
    print(result.head(30))