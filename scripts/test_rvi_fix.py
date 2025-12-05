
import pandas as pd
import numpy as np
import sys
import os

# Add project root to path
sys.path.append('/Users/nachuanchen/Documents/Undergrad_Resources/资产管理与投资策略分析/AMQI')

from factor_library.rvi_value import RVIValueFactor
from factor_library.rvi_cross import RVICrossFactor

def test_rvi_fix():
    print("Testing RVI fix...")
    
    # Create dummy data with High = Low (limit move)
    df = pd.DataFrame({
        'ts_code': ['000001.SZ'] * 10,
        'trade_date': pd.date_range(start='2020-01-01', periods=10),
        'open': [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        'high': [10, 10, 10, 10, 10, 10, 10, 10, 10, 10], # High = Low
        'low':  [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        'close': [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        'vol': [100] * 10
    })
    
    # Test RVI Value
    print("Testing RVIValueFactor...")
    factor = RVIValueFactor()
    try:
        res = factor.calculate(df)
        print("RVIValueFactor calculated successfully.")
        print(res.head())
    except Exception as e:
        print(f"RVIValueFactor failed: {e}")
        
    # Test RVI Cross
    print("\nTesting RVICrossFactor...")
    factor_cross = RVICrossFactor()
    try:
        res_cross = factor_cross.calculate(df)
        print("RVICrossFactor calculated successfully.")
        print(res_cross.head())
    except Exception as e:
        print(f"RVICrossFactor failed: {e}")

if __name__ == "__main__":
    test_rvi_fix()
