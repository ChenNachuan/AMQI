
import pandas as pd
import numpy as np
import statsmodels.api as sm

def test_calc_alpha_beta():
    # Create dummy data
    dates = pd.date_range(start='2020-01-01', periods=100)
    
    # Market return: random normal
    mkt_ret = pd.Series(np.random.normal(0.001, 0.02, 100), index=dates, name='mkt_ret')
    
    # Portfolio return: 1.5 * mkt + 0.002 + noise
    # Alpha should be ~0.002, Beta should be ~1.5
    ls_ret = 1.5 * mkt_ret + 0.002 + np.random.normal(0, 0.005, 100)
    ls_ret.name = 'ls_ret'
    
    # Logic from analyzer.py
    y = ls_ret
    X = mkt_ret
    X = sm.add_constant(X)
    
    model = sm.OLS(y, X, missing='drop').fit()
    
    print("Params index:", model.params.index)
    print("Params values:", model.params.values)
    
    alpha = model.params['const']
    beta_idx_0 = model.params[0]
    beta_idx_1 = model.params[1]
    
    print(f"Alpha (params['const']): {alpha}")
    print(f"Beta (params[0]): {beta_idx_0}")
    print(f"Beta (params[1]): {beta_idx_1}")
    
    if abs(beta_idx_1 - 1.5) < 0.1:
        print("FIX VERIFIED: params[1] is Beta")
    else:
        print("params[1] is NOT Beta")

if __name__ == "__main__":
    test_calc_alpha_beta()
