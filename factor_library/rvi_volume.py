import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class RVIVolumeFactor(BaseFactor):
    """
    RVI-Volume Composite Factor.
    RVI+成交量组合因子 - 结合RVI金叉和成交量放大。
    
    因子含义：
    - 金叉 + 放量：双重确认买入信号，减少假突破
    - factor值越大：RVI越高且放量越多，信号越强
    - 成交量放大验证趋势的真实性
    
    选股逻辑：
    - 做多策略：选择金叉且放量最大的股票
    - 信号质量：放量确认可以过滤假突破
    - 风险控制：无放量的金叉信号被过滤掉（factor=0）
    """
    
    def __init__(self, signal_period: int = 4, volume_ma_period: int = 20):
        """
        Initialize RVI-Volume Factor.
        
        Args:
            signal_period: Signal line period, default 4.
            volume_ma_period: Volume moving average period, default 20.
        """
        self.signal_period = signal_period
        self.volume_ma_period = volume_ma_period
    
    @property
    def name(self) -> str:
        return "RVI_Volume"
        
    @property
    def required_fields(self) -> list:
        return ['open', 'high', 'low', 'close', 'vol']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RVI-Volume Composite Factor.
        
        Formula:
        1. Calculate RVI and Signal
        2. Detect golden cross: RVI crosses above Signal
        3. Calculate volume MA: volume_ma = MA(volume, volume_ma_period)
        4. Volume confirmation: volume > volume_ma
        5. Composite signal: golden_cross AND volume_confirm
        6. factor = RVI × (volume / volume_ma) when conditions met
        
        Args:
            df: Daily dataframe with 'open', 'high', 'low', 'close', 'vol'.
            
        Returns:
            DataFrame with 'RVI_Volume' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        def compute_rvi_volume_for_stock(group):
            """Compute RVI-Volume for a single stock."""
            open_prices = group['open'].values
            high_prices = group['high'].values
            low_prices = group['low'].values
            close_prices = group['close'].values
            volume = group['vol'].values
            
            n = len(close_prices)
            
            # Calculate RVI
            range_hl = high_prices - low_prices
            with np.errstate(divide='ignore', invalid='ignore'):
                vigor = np.divide(
                    close_prices - open_prices,
                    range_hl,
                    out=np.zeros_like(close_prices),
                    where=range_hl != 0
                )
            
            numerator = np.full(n, np.nan)
            for i in range(3, n):
                numerator[i] = (vigor[i-3] + 2*vigor[i-2] + 2*vigor[i-1] + vigor[i]) / 6
            
            denominator = np.full(n, np.nan)
            for i in range(3, n):
                denominator[i] = (range_hl[i-3] + 2*range_hl[i-2] + 2*range_hl[i-1] + range_hl[i]) / 6
            
            with np.errstate(divide='ignore', invalid='ignore'):
                rvi = np.divide(
                    numerator,
                    denominator,
                    out=np.full_like(numerator, np.nan),
                    where=(denominator != 0) & (~np.isnan(denominator))
                )
            
            # Calculate Signal line
            signal = np.full(n, np.nan)
            if self.signal_period == 4:
                for i in range(3, n):
                    if not np.isnan(rvi[i-3:i+1]).any():
                        signal[i] = (rvi[i-3] + 2*rvi[i-2] + 2*rvi[i-1] + rvi[i]) / 6
            else:
                for i in range(self.signal_period-1, n):
                    if not np.isnan(rvi[i-self.signal_period+1:i+1]).any():
                        signal[i] = np.mean(rvi[i-self.signal_period+1:i+1])
            
            # Calculate volume MA
            volume_ma = np.full(n, np.nan)
            for i in range(self.volume_ma_period-1, n):
                volume_ma[i] = np.mean(volume[i-self.volume_ma_period+1:i+1])
            
            # Detect golden cross and combine with volume
            factor = np.zeros(n)
            for i in range(1, n):
                if np.isnan(rvi[i]) or np.isnan(signal[i]) or \
                   np.isnan(rvi[i-1]) or np.isnan(signal[i-1]) or \
                   np.isnan(volume_ma[i]) or volume_ma[i] == 0:
                    factor[i] = 0
                elif rvi[i-1] <= signal[i-1] and rvi[i] > signal[i]:
                    # Golden cross occurred
                    if volume[i] > volume_ma[i]:
                        # Volume confirmation
                        volume_ratio = volume[i] / volume_ma[i]
                        factor[i] = rvi[i] * volume_ratio
                    else:
                        # No volume confirmation
                        factor[i] = 0
                else:
                    factor[i] = 0
            
            return pd.Series(factor, index=group.index)
        
        # Calculate RVI-Volume for each stock
        factor_values = df.groupby('ts_code', group_keys=False).apply(compute_rvi_volume_for_stock)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: factor_values,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
