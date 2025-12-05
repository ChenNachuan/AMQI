
import pandas as pd
import numpy as np
import os
import sys

# Add project root to path
# Add project root to path
root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
print(f"Added {root_path} to sys.path")
print(f"sys.path: {sys.path}")

from data.data_loader import load_data, RAW_DATA_DIR, WHITELIST_PATH
from factor_library import (
    OCFtoNI, APTurnover, APDays, FATurnover, IntCoverage, TaxRate,
    OpAssetChg, EquityRatio, NOAT, FARatio, ROEMomNAGrowth,
    CapexGrowthRate, DebtGrowthRate, DebtYoyGrowth, Epsurplus,
    EquityTurnover, IssuanceGrowthRate, OpCashRatio, OpCostMargin, RevenuePerShare,
    # New additions
    AccrualsToAssets, CagrCapex, EarningsVolatility, EpChange60D,
    InterestCoverageRatio, LogMarketCap, Logffmv, PegDyRatio,
    QuarterlyAbnormalGm, QuarterlyRoic, RoicQoqChange, SalesExpenseRatio,
    StandardizedFinancialDebtChangeRatio, StandardizedOperatingProfit, TotalAssetTurnover
)
from scripts.utils.financial_utils import convert_ytd_to_ttm

def construct_factors():
    print("正在构建基本面因子...")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    factors_dir = os.path.join(base_dir, 'data', 'factors')
    os.makedirs(factors_dir, exist_ok=True)
    
    # 1. Load Market Data (Daily) for alignment and basic factors
    print("正在加载市场日线数据 (后复权)...")
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    daily_adj_path = os.path.join(base_dir, 'data', 'data_cleaner', 'daily_adj.parquet')
    
    try:
        daily = pd.read_parquet(daily_adj_path, columns=['ts_code', 'trade_date', 'hfq_close'], engine='fastparquet')
    except Exception:
        daily = pd.read_parquet(daily_adj_path, columns=['ts_code', 'trade_date', 'hfq_close'])
        
    daily = daily.rename(columns={'hfq_close': 'close'})
    daily_basic = load_data('daily_basic', columns=['total_mv', 'pb'])
    
    # Merge daily and daily_basic
    market_data = pd.merge(daily, daily_basic, on=['ts_code', 'trade_date'], how='inner')
    
    # Resample to Monthly for final output
    print("正在将市场数据重采样为月频...")
    market_data['month'] = market_data['trade_date'].dt.to_period('M')
    market_data = market_data.sort_values('trade_date')
    
    # Calculate Monthly Return (ret) and Size (size)
    # Group by ts_code and month
    print("正在将市场数据重采样为月频 (Vectorized)...")
    
    # 构造聚合字典
    agg_dict = {
        'close': 'last',
        'total_mv': 'last',
        'trade_date': 'last'
    }
    # 如果有 PB 列，也取最后一天
    if 'pb' in market_data.columns:
        agg_dict['pb'] = 'last'
        
    monthly_market = market_data.groupby(['ts_code', 'month']).agg(agg_dict).reset_index()
    monthly_market['ret'] = monthly_market.groupby('ts_code')['close'].pct_change()
    
    # Rename total_mv to size for factor consistency
    monthly_market['size'] = monthly_market['total_mv']
    
    # 2. Load Financial Data
    print("正在加载财务报表数据...")
    
    # Load whitelist to get valid ts_codes
    whitelist = pd.read_parquet(WHITELIST_PATH, columns=['ts_code'])
    valid_stocks = set(whitelist['ts_code'].unique())
    
    def load_financials(filename, columns=None):
        path = os.path.join(RAW_DATA_DIR, filename)
        if not os.path.exists(path):
            print(f"警告: 未找到 {path}。")
            return pd.DataFrame()
        
        # Determine columns to load
        if columns:
            cols_to_load = ['ts_code', 'ann_date', 'end_date'] + columns
            cols_to_load = list(set(cols_to_load))
        else:
            cols_to_load = None # Load all
            
        # Read parquet
        try:
            df = pd.read_parquet(path, columns=cols_to_load)
        except Exception as e:
            print(f"使用 pyarrow 加载 {filename} 失败: {e}。正在尝试使用 fastparquet 并加载所有列...")
            try:
                df = pd.read_parquet(path, engine='fastparquet')
            except Exception as e2:
                print(f"使用 fastparquet 加载 {filename} 失败: {e2}。返回空 DataFrame。")
                return pd.DataFrame()
            
        # Filter by universe
        df = df[df['ts_code'].isin(valid_stocks)]
        # Ensure dates
        if 'ann_date' in df.columns:
            df['ann_date'] = pd.to_datetime(df['ann_date'].astype(str))
        if 'end_date' in df.columns:
            df['end_date'] = pd.to_datetime(df['end_date'].astype(str))
        return df

    # Load specific columns needed for Bm and Ep + others for new factors
    # Note: The new factors classes handle their own column requirements, but we need to ensure
    # the base dataframe passed to them has all needed columns.
    # Since we don't know exactly what columns every factor needs without inspecting them, 
    # we will load the full files or a broad set. 
    # To be safe and simple given the context, let's load all columns for now, 
    # but strictly ensure Bm/Ep columns are present.
    
    bs = load_financials('balancesheet.parquet')
    inc = load_financials('income.parquet')
    cf = load_financials('cashflow.parquet')
    
    # Merge Financials
    print("正在合并财务数据...")
    
    # Start with Income Statement
    financial_df = inc.copy()
    
    # Merge Balance Sheet
    if not bs.empty:
        financial_df = pd.merge(financial_df, bs, on=['ts_code', 'end_date', 'ann_date'], how='outer', suffixes=('', '_bs'))
        
    # Merge Cash Flow
    if not cf.empty:
        financial_df = pd.merge(financial_df, cf, on=['ts_code', 'end_date', 'ann_date'], how='outer', suffixes=('', '_cf'))
        
    # Sort for TTM calculation
    financial_df = financial_df.sort_values(['ts_code', 'end_date'])
    
    # Apply TTM Conversion (Fix Double Counting)
    print("正在将利润表项目的 YTD 转换为 TTM...")
    # Identify columns that need TTM (Flow items from Income Statement)
    # We need to check what columns are present.
    # Common income statement items: revenue, n_income, n_income_attr_p, operate_profit, total_profit, income_tax, int_exp, etc.
    # Also Cash Flow items are YTD.
    
    # Let's get all numeric columns from inc and cf that are not keys
    inc_cols = [c for c in inc.columns if c not in ['ts_code', 'ann_date', 'end_date'] and pd.api.types.is_numeric_dtype(inc[c])]
    cf_cols = [c for c in cf.columns if c not in ['ts_code', 'ann_date', 'end_date'] and pd.api.types.is_numeric_dtype(cf[c])]
    
    # Define fields that strictly require TTM conversion (Flow variables)
    TTM_REQUIRED_FIELDS = {
        'n_cashflow_act', 'n_income', 'int_exp', 'income_tax', 'total_profit', 
        'total_cogs', 'n_income_attr_p', 'revenue', 'total_revenue', 'oper_cost', 'operate_profit',
        'n_recp_disp_fiolta', 'sell_exp', 'admin_exp', 'fin_exp'
    }
    
    # We only convert columns that exist in financial_df AND are in the required list
    cols_to_convert = [c for c in inc_cols + cf_cols if c in financial_df.columns and c in TTM_REQUIRED_FIELDS]
    
    # Remove duplicates
    cols_to_convert = list(set(cols_to_convert))
    
    if cols_to_convert:
        print(f"需要转换的列数: {len(cols_to_convert)}")
        # 必须循环处理每一列，因为 convert_ytd_to_ttm 一次只接受一个字符串列名
        for i, col in enumerate(cols_to_convert):
            if i % 5 == 0:
                print(f"  正在处理第 {i+1}/{len(cols_to_convert)} 列: {col} ...")
            try:
                financial_df = convert_ytd_to_ttm(financial_df, col)
            except Exception as e:
                print(f"  转换列 {col} 失败: {e}")
                
        # 批量重命名：将原始列改为 _ytd，将 _ttm 列改为原始列名
        # 这样下游因子计算代码（如 n_income）不需要改名就能直接用上 TTM 数据
        rename_dict = {}
        for col in cols_to_convert:
            if f'{col}_ttm' in financial_df.columns:
                rename_dict[col] = f'{col}_ytd'
                rename_dict[f'{col}_ttm'] = col
                
        financial_df = financial_df.rename(columns=rename_dict)
        print(f"已完成 TTM 转换。")
    else:
        print("没有需要转换为 TTM 的列。")
        
    # Create Aliases for Factor Compatibility
    print("正在创建列别名...")
    if 'n_income' in financial_df.columns:
        financial_df['net_profit'] = financial_df['n_income']
    if 'operate_profit' in financial_df.columns:
        financial_df['op_income'] = financial_df['operate_profit']
    if 'n_recp_disp_fiolta' in financial_df.columns:
        financial_df['asset_disp_income'] = financial_df['n_recp_disp_fiolta']
        
    # Calculate ROE (TTM) for RoeMomNaGrowth
    # roe_ttm = n_income_attr_p (TTM) / total_hldr_eqy_exc_min_int (Average or End?)
    # Using End period equity for simplicity as per common practice in simple factors, or Average if possible.
    # Here we use End period.
    if 'n_income_attr_p' in financial_df.columns and 'total_hldr_eqy_exc_min_int' in financial_df.columns:
        financial_df['roe_ttm'] = financial_df['n_income_attr_p'] / financial_df['total_hldr_eqy_exc_min_int']
        print("已计算 roe_ttm。")
    else:
        print("警告: 缺少计算 roe_ttm 的列 (n_income_attr_p, total_hldr_eqy_exc_min_int)。")
    
    # 3. Calculate New Factors (Class-based)
    print("正在计算新的基于类的因子...")
    
    factors = [
        OCFtoNI(), APTurnover(), APDays(), FATurnover(), IntCoverage(),
        TaxRate(), OpAssetChg(), EquityRatio(), NOAT(), FARatio(), ROEMomNAGrowth(),
        CapexGrowthRate(), DebtGrowthRate(), DebtYoyGrowth(), Epsurplus(),
        EquityTurnover(), IssuanceGrowthRate(), OpCashRatio(), OpCostMargin(), RevenuePerShare(),
        # New additions
        AccrualsToAssets(), CagrCapex(), EarningsVolatility(), EpChange60D(),
        InterestCoverageRatio(), LogMarketCap(), Logffmv(), PegDyRatio(),
        QuarterlyAbnormalGm(), QuarterlyRoic(), RoicQoqChange(), SalesExpenseRatio(),
        StandardizedFinancialDebtChangeRatio(), StandardizedOperatingProfit(), TotalAssetTurnover()
    ]
    
    # Store results
    factor_results = []
    
    for factor in factors:
        print(f"正在计算 {factor.name}...")
        try:
            # Check if required columns exist (simple check, factor might fail inside otherwise)
            # We rely on the try-except block to handle missing columns gracefully
            res = factor.calculate(financial_df)
            factor_results.append(res)
        except Exception as e:
            print(f"计算 {factor.name} 时出错: {e}")
            # We can append an empty DF or just skip. Skipping is safer.
            
    # Merge all factor results
    if not factor_results:
        print("没有计算出新的因子。")
        all_factors = financial_df[['ts_code', 'ann_date', 'end_date']].copy()
    else:
        all_factors = factor_results[0]
        for i in range(1, len(factor_results)):
            all_factors = pd.merge(all_factors, factor_results[i], on=['ts_code', 'end_date', 'ann_date'], how='outer')
    
    # Add raw fields needed for Bm and Ep to all_factors so they survive the merge
    # Bm needs: total_hldr_eqy_exc_min_int (from BS)
    # Ep needs: n_income_attr_p (from Income)
    
    cols_to_preserve = []
    if 'total_hldr_eqy_exc_min_int' in financial_df.columns:
        cols_to_preserve.append('total_hldr_eqy_exc_min_int')
    if 'n_income_attr_p' in financial_df.columns:
        cols_to_preserve.append('n_income_attr_p')
        
    if cols_to_preserve:
        # Merge these columns into all_factors
        # We need to be careful about duplicates if all_factors already has them (unlikely from factor calc)
        temp_df = financial_df[['ts_code', 'end_date', 'ann_date'] + cols_to_preserve]
        all_factors = pd.merge(all_factors, temp_df, on=['ts_code', 'end_date', 'ann_date'], how='left')

    # 4. Merge to Market Data (Avoid Look-ahead Bias)
    print("正在将因子合并到市场数据 (asof)...")
    
    # Ensure ann_date is valid
    all_factors = all_factors.dropna(subset=['ann_date']).sort_values('ann_date')
    monthly_market = monthly_market.sort_values('trade_date')
    
    # Merge using merge_asof
    merged = pd.merge_asof(
        monthly_market,
        all_factors,
        left_on='trade_date',
        right_on='ann_date',
        by='ts_code',
        direction='backward'
    )
    
    # 5. Calculate Bm and Ep
    print("正在计算 Bm 和 Ep...")
    
    # Bm = Book / Market
    # User requested to use 1 / pb from daily_basic
    if 'pb' in merged.columns:
        merged['Bm'] = 1 / merged['pb']
    else:
        print("警告: 计算 Bm 缺少必要列 'pb'。")
        merged['Bm'] = np.nan
        
    # Ep = Earnings / Price (Market Cap)
    if 'n_income_attr_p' in merged.columns and 'total_mv' in merged.columns:
        merged['Ep'] = merged['n_income_attr_p'] / merged['total_mv']
    else:
        print("警告: 计算 Ep 缺少必要列。")
        merged['Ep'] = np.nan

    # 6. Save
    output_path = os.path.join(factors_dir, 'fundamental_factors.parquet')
    print(f"正在保存至 {output_path}...")
    
    # Select columns
    # Basic: ts_code, trade_date, month, ret, size
    # Original: Bm, Ep
    # New: [f.name for f in factors]
    
    base_cols = ['ts_code', 'trade_date', 'month', 'ret', 'size', 'Bm', 'Ep']
    new_factor_cols = [f.name for f in factors]
    
    cols_to_keep = base_cols + new_factor_cols
    
    # Filter existing columns
    cols_to_keep = [c for c in cols_to_keep if c in merged.columns]
    
    final_df = merged[cols_to_keep].copy()
    final_df = final_df.set_index(['trade_date', 'ts_code']).sort_index()
    
    final_df.to_parquet(output_path)
    print("完成。")
    print("样例输出:")
    print(final_df.tail())

if __name__ == "__main__":
    construct_factors()