import pandas as pd
import numpy as np
import sys
from pathlib import Path

# 添加父目录到路径以导入BaseFactor
factor_lib_path = Path(__file__).parent.parent if '__file__' in locals() else Path.cwd().parent
sys.path.insert(0, str(factor_lib_path))

from .base_factor import BaseFactor


class MonthlyExcessReturnSeasonalReversal(BaseFactor):
    """
    月度超额收益季节性反转因子 (MERSR)
    
    在t月月末，计算过去t-1月至t-11月（不包含去年同月）的月度超额收益均值。
    该因子利用股票收益的月度反转效应，过去特定月份的超额收益与未来该月的超额收益之间存在负相关关系。
    
    注意：该因子基于月末计算，因子值在下个月初可用（避免未来信息泄露）
    """
    
    def __init__(self, lookback_months=11):
        """
        Args:
            lookback_months: 回溯月份数，默认为11（计算过去1-11个月，不含去年同月）
        """
        self.lookback_months = lookback_months
    
    @property
    def name(self) -> str:
        return f"MERSR_{self.lookback_months}M"
        
    @property
    def required_fields(self) -> list:
        # 只需要close字段，其他字段(ts_code, trade_date)通过check_dependencies验证存在即可
        return ['close']
        
    def calculate(self, df: pd.DataFrame, market_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        计算月度超额收益季节性反转因子
        
        Args:
            df: 包含股票日线数据的DataFrame，需要有'close', 'trade_date', 'ts_code'字段
            market_df: 市场指数数据（可选），需要有'close', 'trade_date'字段
                      如果为None，则使用全市场股票的等权平均收益率作为基准
            
        Returns:
            DataFrame，包含因子值，索引为[trade_date, ts_code]
            每日返回因子值（月末计算的值会前向填充到下月所有交易日）
        """
        self.check_dependencies(df)
        
        # 确保必要的列存在
        if 'ts_code' not in df.columns or 'trade_date' not in df.columns:
            raise ValueError("DataFrame必须包含'ts_code'和'trade_date'列")
        
        # 确保数据已排序
        df = df.sort_values(['ts_code', 'trade_date']).copy()
        
        # 确保trade_date是datetime格式
        if df['trade_date'].dtype == 'object':
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        elif df['trade_date'].dtype == 'int64':
            df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
        
        # 添加年月字段
        df['year_month'] = df['trade_date'].dt.to_period('M')
        df['month'] = df['trade_date'].dt.month
        
        # 计算每只股票的月末收盘价（使用每月最后一个交易日）
        monthly_close = df.groupby(['ts_code', 'year_month']).agg({
            'close': 'last',
            'trade_date': 'last',
            'month': 'last'
        }).reset_index()
        
        # 计算月度收益率
        monthly_close = monthly_close.sort_values(['ts_code', 'year_month'])
        monthly_close['stock_monthly_return'] = monthly_close.groupby('ts_code')['close'].pct_change()
        
        # 计算市场月度收益率
        if market_df is not None:
            # 如果提供了市场数据，使用市场指数计算基准收益率
            market_df = market_df.copy()
            if market_df['trade_date'].dtype == 'object':
                market_df['trade_date'] = pd.to_datetime(market_df['trade_date'], format='%Y%m%d')
            elif market_df['trade_date'].dtype == 'int64':
                market_df['trade_date'] = pd.to_datetime(market_df['trade_date'].astype(str), format='%Y%m%d')
            
            market_df['year_month'] = market_df['trade_date'].dt.to_period('M')
            market_monthly = market_df.groupby('year_month').agg({
                'close': 'last'
            }).reset_index()
            market_monthly = market_monthly.sort_values('year_month')
            market_monthly['market_monthly_return'] = market_monthly['close'].pct_change()
            
            # 合并市场收益率
            monthly_close = monthly_close.merge(
                market_monthly[['year_month', 'market_monthly_return']], 
                on='year_month', 
                how='left'
            )
        else:
            # 如果没有市场数据，使用所有股票的等权平均收益率作为市场收益率
            market_return = monthly_close.groupby('year_month')['stock_monthly_return'].mean().reset_index()
            market_return.columns = ['year_month', 'market_monthly_return']
            monthly_close = monthly_close.merge(market_return, on='year_month', how='left')
        
        # 计算月度超额收益
        monthly_close['excess_return'] = monthly_close['stock_monthly_return'] - monthly_close['market_monthly_return']
        
        # 计算季节性反转因子
        # 对于每个月末时间点t，计算过去1-11个月（排除去年同月）的超额收益均值
        result_list = []
        
        for ts_code in monthly_close['ts_code'].unique():
            stock_data = monthly_close[monthly_close['ts_code'] == ts_code].copy()
            stock_data = stock_data.sort_values('year_month')
            
            factor_values = []
            year_months = []
            
            for idx, row in stock_data.iterrows():
                current_month = row['month']
                current_year_month = row['year_month']
                
                # 获取过去的数据（需要足够的历史数据）
                past_data = stock_data[stock_data['year_month'] < current_year_month]
                
                if len(past_data) >= self.lookback_months:
                    # 排除与当前月份相同的历史月份（季节性调整）
                    past_data_filtered = past_data[past_data['month'] != current_month]
                    
                    # 取最近的lookback_months个月数据
                    past_data_filtered = past_data_filtered.tail(self.lookback_months)
                    
                    # 计算超额收益均值
                    if len(past_data_filtered) > 0:
                        factor_value = past_data_filtered['excess_return'].mean()
                    else:
                        factor_value = np.nan
                else:
                    factor_value = np.nan
                
                factor_values.append(factor_value)
                year_months.append(current_year_month)
            
            temp_df = pd.DataFrame({
                'ts_code': ts_code,
                'year_month': year_months,
                self.name: factor_values
            })
            result_list.append(temp_df)
        
        # 合并所有股票的结果
        monthly_factors = pd.concat(result_list, ignore_index=True)
        
        # 将月度因子值扩展到每日数据
        # 关键：在t月月末计算的因子，应该在t+1月初开始使用（避免前视偏差）
        df_with_month = df[['ts_code', 'trade_date', 'year_month']].copy()
        
        # 将因子值向后移一个月（月末计算的值用于下个月）
        monthly_factors['year_month_shifted'] = monthly_factors['year_month'].apply(lambda x: x + 1)
        
        # 合并到日度数据
        result = df_with_month.merge(
            monthly_factors[['ts_code', 'year_month_shifted', self.name]],
            left_on=['ts_code', 'year_month'],
            right_on=['ts_code', 'year_month_shifted'],
            how='left'
        )
        
        # 清理辅助列
        result = result[['ts_code', 'trade_date', self.name]]
        
        # 对于每只股票，前向填充因子值（月内所有交易日使用同一因子值）
        result = result.sort_values(['ts_code', 'trade_date'])
        result[self.name] = result.groupby('ts_code')[self.name].ffill()
        
        # 设置索引
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result