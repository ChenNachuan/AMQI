import pandas as pd
import numpy as np
import sys
sys.path.append('..')
from .base_factor import BaseFactor

class CVaR(BaseFactor):
    """
    条件风险价值 (CVaR) 因子
    
    CVaR 衡量在最坏 α% 情况下的预期损失。
    它被计算为超过 VaR 阈值的损失的平均值。
    
    公式:
    CVaR_α = E[X | X ≤ VaR_α]
    
    其中:
    - α: 置信水平 (默认 0.05，代表最坏的 5% 情况)
    - X: 投资组合损失 (收益的负值)
    - VaR_α: 置信水平 α 下的风险价值
    """
    
    def __init__(self, alpha: float = 0.05, window: int = 60):
        """
        初始化 CVaR 因子
        
        参数:
            alpha: CVaR 计算的置信水平 (默认 0.05)
            window: 滚动窗口大小，单位为天 (默认 60 天，约 3 个月)
        """
        super().__init__()
        self.alpha = alpha
        self.window = window
    
    @property
    def name(self) -> str:
        return "CVaR"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 CVaR 因子
        
        参数:
            df: 包含 'close' 价格数据的 DataFrame
            
        返回:
            包含 'CVaR' 列的 DataFrame
        """
        self.check_dependencies(df)
        
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 计算日收益率
        df['returns'] = df.groupby('ts_code')['close'].pct_change()
        
        # 使用滚动窗口计算每只股票的 CVaR
        def calculate_cvar(returns_series):
            """
            计算收益率序列的 CVaR
            
            参数:
                returns_series: 收益率序列
                
            返回:
                CVaR 值（正值表示损失）
            """
            if len(returns_series) < self.window or returns_series.isna().all():
                return np.nan
            
            # 移除 NaN 值
            valid_returns = returns_series.dropna()
            
            if len(valid_returns) < 2:
                return np.nan
            
            # 计算损失（收益的负值，正值表示损失）
            losses = -valid_returns
            
            # 计算 VaR: alpha 分位数，表示最坏 alpha% 情况的阈值
            # 对于损失，我们要找最大的 alpha% 损失
            var_threshold = np.percentile(losses, (1 - self.alpha) * 100)
            
            # 计算 CVaR: 损失超过 VaR 阈值的平均值
            # 即最坏 alpha% 情况下的平均损失
            # CVaR_α = E[Loss | Loss ≥ VaR_α]
            tail_losses = losses[losses >= var_threshold]
            
            if len(tail_losses) == 0:
                # 如果没有超过阈值的损失，返回阈值本身
                return var_threshold
            
            cvar = tail_losses.mean()
            
            return cvar
        
        # 应用滚动 CVaR 计算
        cvar = df.groupby('ts_code')['returns'].rolling(
            window=self.window, 
            min_periods=int(self.window * 0.8)
        ).apply(calculate_cvar, raw=False).reset_index(0, drop=True)
        
        # 创建结果 DataFrame
        result = pd.DataFrame({
            self.name: cvar,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result