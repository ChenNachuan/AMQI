import sys
import os
from pathlib import Path

# 添加项目根目录到路径
project_root = Path("../..").resolve()
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
from data.data_loader import load_data

# 加载daily_basic数据，包含turnover_rate字段
df = load_data(
    dataset_name='daily_basic',
    columns=['ts_code', 'trade_date', 'turnover_rate'],
    start_date='20000101',
    end_date='20251231',
    filter_universe=True
)


from factor_library.base_factor import BaseFactor

class MonthlyTurnover(BaseFactor):
    """
    月均换手率因子 (Monthly Turnover Rate Factor).
    计算过去一个月内每日换手率的平均值。
    
    公式: Monthly Turnover Rate = (1/N) * Σ(daily_turnover_rate_i)
    其中 N 为当月交易日总天数
    
    注意: 
    - turnover_rate 字段来自 daily_basic.parquet
    - 该字段单位为百分比(%)，例如 5.23 表示 5.23%
    - 使用滚动20个交易日窗口计算月均值（约1个自然月）
    """
    
    @property
    def name(self) -> str:
        return "MonthlyTurnover"
        
    @property
    def required_fields(self) -> list:
        return ['turnover_rate']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算月均换手率因子
        
        Args:
            df: DataFrame，必须包含 'ts_code', 'trade_date', 'turnover_rate' 列
            
        Returns:
            DataFrame，包含 'MonthlyTurnover' 因子值
        """
        # 检查必要字段
        self.check_dependencies(df)
        
        # 确保数据按股票代码和交易日期排序
        df = df.sort_values(['ts_code', 'trade_date']).copy()
        
        # 数据验证：检查turnover_rate是否有效
        if df['turnover_rate'].isna().all():
            raise ValueError("turnover_rate 字段全部为空，请检查数据源")
        
        # 设置窗口期为20个交易日（约一个月）
        window = 20
        # 设置 min_periods=10 以减少数据损失（至少需要10天数据）
        min_periods = 10
        
        print(f"计算窗口: {window}个交易日")
        print(f"最小周期: {min_periods}个交易日")
        print(f"数据有效率: {(~df['turnover_rate'].isna()).sum() / len(df) * 100:.2f}%")
        
        # 计算滚动平均换手率
        monthly_turnover = df.groupby('ts_code')['turnover_rate'].rolling(
            window=window, 
            min_periods=min_periods  # 改进: 降低最小周期要求
        ).mean().reset_index(level=0, drop=True)
        
        # 构建结果DataFrame
        result = pd.DataFrame({
            self.name: monthly_turnover,
            'trade_date': df['trade_date'],
            'ts_code': df['ts_code']
        })
        
        # 设置多重索引并排序
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        # 输出统计信息
        valid_count = (~result[self.name].isna()).sum()
        total_count = len(result)
        print(f"\n因子计算完成:")
        print(f"- 有效因子值: {valid_count:,} ({valid_count/total_count*100:.2f}%)")
        print(f"- 缺失值: {total_count - valid_count:,} ({(total_count-valid_count)/total_count*100:.2f}%)")
        
        return result



