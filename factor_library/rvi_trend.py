import pandas as pd
import numpy as np
from .base_factor import BaseFactor

class RVITrendFactor(BaseFactor):
    """
    RVI-Trend Composite Factor.
    RVI+趋势组合因子 - 结合RVI金叉和价格趋势。
    
    因子含义：
    - 金叉 + 趋势向上：顺势交易，捕捉趋势中的动能增强
    - factor值越大：RVI越高且价格越强势
    - 避免逆势交易，提高胜率
    
    选股逻辑：
    - 做多策略：选择金叉且价格在均线上方的股票（顺势）
    - 风险控制：过滤掉逆势的金叉信号
    - 趋势跟随：只做趋势中的加速行情
    """
    
    def __init__(self, signal_period: int = 4, trend_ma_period: int = 20):
        """
        Initialize RVI-Trend Factor.
        
        Args:
            signal_period: Signal line period, default 4.
            trend_ma_period: Trend moving average period, default 20.
        """
        self.signal_period = signal_period
        self.trend_ma_period = trend_ma_period
    
    @property
    def name(self) -> str:
        return "RVI_Trend"
        
    @property
    def required_fields(self) -> list:
        return ['open', 'high', 'low', 'close']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate RVI-Trend Composite Factor.
        
        Formula:
        1. Calculate RVI and Signal
        2. Detect golden cross: RVI crosses above Signal
        3. Calculate price MA: price_ma = MA(close, trend_ma_period)
        4. Trend confirmation: close > price_ma
        5. Composite signal: golden_cross AND trend_confirm
        6. factor = RVI × (1 + (close - price_ma) / price_ma) when conditions met
        
        Args:
            df: Daily dataframe with 'open', 'high', 'low', 'close'.
            
        Returns:
            DataFrame with 'RVI_Trend' column.
        """
        self.check_dependencies(df)
        
        # Ensure sorted
        df = df.sort_values(['ts_code', 'trade_date'])
        
        def compute_rvi_trend_for_stock(group):
            """Compute RVI-Trend for a single stock."""
            open_prices = group['open'].values
            high_prices = group['high'].values
            low_prices = group['low'].values
            close_prices = group['close'].values
            
            n = len(close_prices)
            
            # Calculate RVI
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
            
            # Calculate price MA
            price_ma = np.full(n, np.nan)
            for i in range(self.trend_ma_period-1, n):
                price_ma[i] = np.mean(close_prices[i-self.trend_ma_period+1:i+1])
            
            # Detect golden cross and combine with trend
            factor = np.zeros(n)
            for i in range(1, n):
                if np.isnan(rvi[i]) or np.isnan(signal[i]) or \
                   np.isnan(rvi[i-1]) or np.isnan(signal[i-1]) or \
                   np.isnan(price_ma[i]) or price_ma[i] == 0:
                    factor[i] = 0
                elif rvi[i-1] <= signal[i-1] and rvi[i] > signal[i]:
                    # Golden cross occurred
                    if close_prices[i] > price_ma[i]:
                        # Trend confirmation
                        price_strength = (close_prices[i] - price_ma[i]) / price_ma[i]
                        factor[i] = rvi[i] * (1 + price_strength)
                    else:
                        # Against trend
                        factor[i] = 0
                else:
                    factor[i] = 0
            
            return pd.Series(factor, index=group.index)
        
        # Calculate RVI-Trend for each stock
        factor_values = df.groupby('ts_code', group_keys=False).apply(compute_rvi_trend_for_stock)
        
        # Prepare result
        result = pd.DataFrame({
            self.name: factor_values,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result
