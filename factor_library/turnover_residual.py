import pandas as pd
import numpy as np
from scipy import stats
import sys
from pathlib import Path

# 添加factor_library到路径
sys.path.append(str(Path('.').absolute().parent))
from .base_factor import BaseFactor

class LiquidityMktCapNeutralTurnover(BaseFactor):
    """
    市值中性化换手率残差因子 (Liq_MktCap_Neutral_Turnover)
    
    通过横截面回归模型剔除市值对换手率的影响，得到的残差即为因子值。
    回归模型: ln(Turnover_{i,t}) = α_t + β_t * ln(MktValue_{i,t}) + ε_{i,t}
    
    因子值 = ε_{i,t} (回归残差)
    """
    
    @property
    def name(self) -> str:
        return "Liq_MktCap_Neutral_Turnover"
        
    @property
    def required_fields(self) -> list:
        """需要换手率和流通市值"""
        return ['turnover_rate', 'circ_mv']
        
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算市值中性化换手率残差因子
        
        Args:
            df: DataFrame with 'turnover_rate' and 'circ_mv'
                trade_date 应为整数格式 (例如 20241201)
            
        Returns:
            DataFrame with factor column, indexed by [trade_date, ts_code]
        """
        self.check_dependencies(df)
        
        # 复制数据避免修改原始数据
        df = df.copy()
        
        # 确保排序
        df = df.sort_values(['ts_code', 'trade_date'])
        
        # 计算过去20个交易日（约1个月）的日均换手率
        window = 20
        df['avg_turnover'] = df.groupby('ts_code')['turnover_rate'].rolling(window).mean().reset_index(0, drop=True)
        
        # 数据清洗：去除无效值
        # 1. 去除NaN值
        df = df.dropna(subset=['avg_turnover', 'circ_mv'])
        
        # 2. 去除非正值（对数计算要求正数）
        df = df[(df['avg_turnover'] > 0) & (df['circ_mv'] > 0)]
        
        # 3. 去除极端异常值（可选，防止回归受极端值影响）
        # 这里使用3倍标准差作为阈值
        for col in ['avg_turnover', 'circ_mv']:
            mean_val = df[col].mean()
            std_val = df[col].std()
            df = df[(df[col] >= mean_val - 3*std_val) & (df[col] <= mean_val + 3*std_val)]
        
        # 对换手率和市值取对数
        df['ln_turnover'] = np.log(df['avg_turnover'])
        df['ln_mktcap'] = np.log(df['circ_mv'])
        
        # 按日期分组进行横截面回归
        residuals = []
        
        for date, group in df.groupby('trade_date'):
            # 确保有足够的样本进行可靠的回归（建议至少30个样本）
            if len(group) < 30:
                continue
                
            # 横截面回归: ln(turnover) = α + β * ln(mktcap) + ε
            X = group['ln_mktcap'].values.reshape(-1, 1)
            y = group['ln_turnover'].values
            
            # 添加截距项
            X_with_intercept = np.column_stack([np.ones(len(X)), X])
            
            try:
                # 使用最小二乘法进行回归
                beta = np.linalg.lstsq(X_with_intercept, y, rcond=None)[0]
                
                # 计算残差
                y_pred = X_with_intercept @ beta
                residual = y - y_pred
                
                # 保存残差（保持原始日期格式）
                temp_df = pd.DataFrame({
                    'trade_date': date,
                    'ts_code': group['ts_code'].values,
                    self.name: residual
                })
                residuals.append(temp_df)
                
            except np.linalg.LinAlgError:
                # 如果回归失败，跳过该日期
                continue
        
        if not residuals:
            raise ValueError("无法计算因子：所有日期的回归都失败了")
        
        # 合并结果
        result = pd.concat(residuals, ignore_index=True)
        
        # 设置索引并排序
        result = result.set_index(['trade_date', 'ts_code']).sort_index()
        
        return result