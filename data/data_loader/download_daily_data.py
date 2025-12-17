"""
Download daily trading data from Tushare using a daily loop to avoid timeouts/limits.
Includes: Daily, DailyBasic, AdjFactor.
"""
import sys
import time
import datetime
from pathlib import Path
import pandas as pd
import tushare as ts

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.data_loader.utils import init_tushare_api, save_to_parquet

# 默认时间范围 (2005-01-01 至今)
START_DATE = '20230101'
END_DATE = '20251215'

def download_daily_by_date(start_date=START_DATE, end_date=END_DATE):
    """
    按日循环下载高频数据 (Daily, DailyBasic, AdjFactor)，避免 API 单次请求限制导致的截断。
    """
    print(f"\n--- 开始按日下载循环 ({start_date} 至 {end_date}) ---")
    
    # Initialize API
    pro = init_tushare_api()
    
    # 1. 获取交易日历
    print("正在获取交易日历...")
    try:
        cal = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open='1')
        trade_dates = cal['cal_date'].tolist()
        print(f"共获取到 {len(trade_dates)} 个交易日。")
    except Exception as e:
        print(f"获取交易日历失败: {e}")
        return

    # 初始化列表用于存储数据
    daily_list = []
    daily_basic_list = []
    adj_factor_list = []
    
    # 2. 按日循环下载
    total_days = len(trade_dates)
    for i, date in enumerate(trade_dates):
        print(f"[{i+1}/{total_days}] 正在下载 {date} ...", end='\r')
        
        try:
            # (1) 日线行情 (Daily)
            # 包含: ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
            df_daily = pro.daily(trade_date=date)
            if not df_daily.empty:
                daily_list.append(df_daily)
            
            # (2) 每日指标 (Daily Basic)
            # 包含: ts_code, trade_date, turnover_rate, pe, pb, total_mv 等
            df_basic = pro.daily_basic(trade_date=date)
            if not df_basic.empty:
                daily_basic_list.append(df_basic)
                
            # (3) 复权因子 (Adj Factor)
            # 包含: ts_code, trade_date, adj_factor
            df_adj = pro.adj_factor(trade_date=date)
            if not df_adj.empty:
                adj_factor_list.append(df_adj)
                
        except Exception as e:
            print(f"\n日期 {date} 下载出错: {e}")
            # 出错不中断，继续下一天
            
        # 频率限制 (避免触发 Tushare 限制)
        time.sleep(0.4) 

    print(f"\n下载循环结束。正在合并并保存数据...")

    # 3. 合并并保存 (Parquet)
    
    # 保存 Daily
    if daily_list:
        print("正在保存 daily.parquet ...")
        full_daily = pd.concat(daily_list, ignore_index=True)
        # 确保按日期和代码排序
        full_daily = full_daily.sort_values(['trade_date', 'ts_code'])
        save_to_parquet(full_daily, 'daily')
    else:
        print("警告: 未下载到 daily 数据。")

    # 保存 Daily Basic
    if daily_basic_list:
        print("正在保存 daily_basic.parquet ...")
        full_basic = pd.concat(daily_basic_list, ignore_index=True)
        full_basic = full_basic.sort_values(['trade_date', 'ts_code'])
        save_to_parquet(full_basic, 'daily_basic')
    else:
        print("警告: 未下载到 daily_basic 数据。")

    # 保存 Adj Factor
    if adj_factor_list:
        print("正在保存 adj_factor.parquet ...")
        full_adj = pd.concat(adj_factor_list, ignore_index=True)
        full_adj = full_adj.sort_values(['trade_date', 'ts_code'])
        save_to_parquet(full_adj, 'adj_factor')
    else:
        print("警告: 未下载到 adj_factor 数据。")

    print("\n所有任务完成。")

if __name__ == "__main__":
    # 可以在此处修改日期范围进行测试，例如下载最近一个月
    # download_daily_by_date(start_date='20230101', end_date='20230131')
    
    # 默认下载全量 (慎用，耗时较长)
    # 为了测试，我们默认只下载最近一周的数据，用户可以手动修改
    # download_daily_by_date()
    
    # 临时测试: 下载 2023 年 1 月的数据
    download_daily_by_date()
