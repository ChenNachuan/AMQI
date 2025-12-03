import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

def verify_risk_factors():
    print("Verifying run_risk_factors...")
    try:
        from scripts.factors.run_risk_factors import run_risk_factors
        # We won't run the full calculation as it might be heavy, 
        # but we can check if it imports and maybe mock the data loading if needed.
        # However, the user wants to ensure it works.
        # Let's try running it. If it fails, we'll know.
        print("Import successful. Starting execution...")
        run_risk_factors()
        print("✓ run_risk_factors executed successfully")
    except Exception as e:
        print(f"✗ run_risk_factors failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_risk_factors()
