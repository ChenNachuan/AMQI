"""
Download Market Index Monthly Data.
"""
import sys
from pathlib import Path
import pandas as pd
import time

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data.data_loader.utils import init_tushare_api, save_to_parquet, _get_raw_data_dir

# Configuration
START_DATE = '20230101'
END_DATE = '20251215'

# Key Indices
KEY_INDICES = ['000300.SH', '000905.SH', '000001.SH']


def download_index_monthly_data(pro):
    print("\n--- 下载指数月度数据 ---")
    print(f"正在下载指数月度行情数据 {KEY_INDICES}...")

    all_monthly = []
    for code in KEY_INDICES:
        print(f"  正在获取 {code} 的月度行情数据...")
        try:
            # 获取指数月线数据
            df_m = pro.index_monthly(
                ts_code=code,
                start_date=START_DATE,
                end_date=END_DATE
            )

            if not df_m.empty:
                # 确保数据按日期排序
                df_m = df_m.sort_values('trade_date')
                all_monthly.append(df_m)
                print(f"  成功获取 {code} 的 {len(df_m)} 个月度数据点")
            else:
                print(f"  {code} 月度数据为空")

        except Exception as e:
            print(f"  获取 {code} 月度数据失败: {e}")
        time.sleep(0.5)  # 避免API调用频率过高

    if all_monthly:
        df_monthly = pd.concat(all_monthly, ignore_index=True)
        # 保存数据
        save_to_parquet(df_monthly, 'index_monthly')
        print(f"已保存 {len(df_monthly)} 条指数月度数据")

    else:
        print("未获取到任何指数月度数据")


def main():
    try:
        pro = init_tushare_api()
    except Exception as e:
        print(f"初始化错误: {e}")
        return

    download_index_monthly_data(pro)
    print("\n指数月度数据下载已完成。")


if __name__ == "__main__":
    main()