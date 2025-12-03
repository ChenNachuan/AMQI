
import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class BollingerSqueezeExpansion(BaseFactor):
    """
    Bollinger Bands Squeeze Expansion Factor.
    缩口扩张策略：捕捉波动率扩张初期，可能出现大行情
    
    选股逻辑：
    1. 前期带宽处于历史低位：bb_width_percentile < 0.2（即处于最窄的20%）
    2. 当前带宽开始扩张：bb_width_change > 0.1（即扩张超过10%）
    3. 价格位置适中：0.3 < percent_b < 0.7
    """
    
    @property
    def name(self) -> str:
        return "Boll_Squeeze_Expansion"
        
    @property
    def required_fields(self) -> list:
        return ['close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Bollinger Bands Squeeze Expansion Signal.
        
        Args:
            df: Daily dataframe with 'close'.
            
        Returns:
            DataFrame with 'Boll_Squeeze_Expansion' column (1 for signal, 0 for no signal).
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        window = 20
        num_std = 2
        bb_width_percentile_threshold = 0.2
        
        # Calculate Bollinger Bands
        rolling = df.groupby('ts_code')['close'].rolling(window)
        ma = rolling.mean().reset_index(0, drop=True)
        std = rolling.std().reset_index(0, drop=True)
        
        upper = ma + num_std * std
        lower = ma - num_std * std
        
        # Calculate BB Width (normalized)
        bb_width = (upper - lower) / ma
        
        # Calculate BB Width percentile (using past window*3 days)
        bb_width_percentile = df.groupby('ts_code').apply(
            lambda group: group.assign(
                bb_width_pct=((upper.loc[group.index] - lower.loc[group.index]) / ma.loc[group.index])
                .rolling(window=window*3, min_periods=window)
                .apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1] if len(x) >= window else np.nan, raw=False)
            )['bb_width_pct']
        ).reset_index(0, drop=True)
        
        # Calculate BB Width change
        bb_width_prev = df.groupby('ts_code').apply(
            lambda group: ((upper.loc[group.index] - lower.loc[group.index]) / ma.loc[group.index]).shift(1)
        ).reset_index(0, drop=True)
        
        bb_width_change = (bb_width / bb_width_prev) - 1
        
        # Calculate Percent B
        denominator = upper - lower
        percent_b = (df['close'] - lower) / denominator
        
        # Generate signal
        signal = (
            (bb_width_percentile.shift(1) < bb_width_percentile_threshold) &  # 前期带宽低位
            (bb_width_change > 0.1) &                                          # 带宽扩张>10%
            (percent_b > 0.3) &                                                # 价格不能太低
            (percent_b < 0.7)                                                  # 价格不能太高
        ).astype(int)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: signal,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
