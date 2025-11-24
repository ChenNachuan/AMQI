
import sys
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from data_loader import load_stock_data
from factor_library.momentum import MomentumFactor
from backtest_engine.backtester import Backtester
from model_assembly.linear_model import LinearModel

def verify_workflow():
    print("1. Loading Data...")
    try:
        df = load_stock_data(project_root / "data")
        print(f"Data loaded: {df.shape}")
    except Exception as e:
        print(f"Data loading failed: {e}")
        return

    print("\n2. Calculating Factor...")
    try:
        mom_factor = MomentumFactor()
        signal = mom_factor.calculate(df)
        print(f"Signal calculated: {signal.shape}")
        print(signal.head())
    except Exception as e:
        print(f"Factor calculation failed: {e}")
        return

    print("\n3. Testing Model Assembly...")
    try:
        # Just testing the mechanism with one factor
        factors = pd.DataFrame({'momentum': signal})
        model = LinearModel(weights={'momentum': 1.0})
        combined_signal = model.combine(factors)
        print(f"Combined signal: {combined_signal.shape}")
    except Exception as e:
        print(f"Model assembly failed: {e}")
        return

    print("\n4. Running Backtest...")
    try:
        backtester = Backtester(df)
        stats = backtester.run_backtest(combined_signal, quantiles=5)
        print("Backtest stats:")
        for k, v in stats.items():
            print(f"{k}: {v}")
    except Exception as e:
        print(f"Backtest failed: {e}")
        return

    print("\nVerification Successful!")

if __name__ == "__main__":
    verify_workflow()
