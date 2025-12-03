
import os
import json

def create_notebook_content(factor_name):
    """
    Creates the content of the Jupyter notebook for a given factor.
    """
    
    # Define the cells based on the user's template
    cells = [
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "import sys\n",
                "import os\n",
                "import pandas as pd\n",
                "import matplotlib.pyplot as plt\n",
                "\n",
                "# Add project root to system path to allow importing modules\n",
                "sys.path.append(os.path.abspath('..'))\n",
                "\n",
                "from backtest.engine import BacktestEngine"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Load the prepared datasets\n",
                "df = pd.read_parquet('../data/final_dataset.parquet')\n",
                "bench_df = pd.read_parquet('../data/benchmark_csi300_monthly.parquet')\n",
                "\n",
                "print(f\"Data Loaded. Shape: {df.shape}\")"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                f"FACTOR_NAME = '{factor_name}'  # This will be replaced by the script\n",
                "print(f\"Analyzing Factor: {FACTOR_NAME}\")\n",
                "\n",
                "# Initialize Engine with Benchmark\n",
                "engine = BacktestEngine(df, factor_name=FACTOR_NAME, benchmark_df=bench_df)"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Run analysis using Value-Weighted sorting (Academic Standard)\n",
                "summary = engine.run_analysis(weighting='vw')\n",
                "\n",
                "# Display Key Metrics\n",
                "print(\"Performance Summary:\")\n",
                "for k, v in summary.items():\n",
                "    print(f\"{k}: {v:.4f}\")"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Plot cumulative returns and IC series\n",
                "engine.plot_results()\n",
                "plt.show()"
            ]
        }
    ]
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.5"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    return notebook

def main():
    # Define Factor List
    fundamental_factors = [
        'R11', 'Bm', 'Ep', 'Roe', 'size',
        'OCFtoNI', 'APTurnover', 'APDays', 'FATurnover', 'IntCoverage',
        'TaxRate', 'OpAssetChg', 'EquityRatio', 'NOAT', 'FARatio'
    ]
    risk_trading_factors = ['beta', 'IVFF', 'TUR', 'Srev']
    technical_factors = [
        'ATR', 'Bollinger', 'Ichimoku', 'MFI', 'OBV', 'PVT', 'RVI', 'TEMA', 'SWMA',
        # New Factors
        'ATR_Expansion', 'ATR_Price_Breakout', 'ATR_Price_Position', 'ATR_Trend', 'ATR_Volume_Confirmation',
        'Bollinger_Breakout_Upper', 'Bollinger_Middle_Support', 'Bollinger_Oversold_Bounce', 'Bollinger_Squeeze_Expansion',
        'Ichimoku_Cloud_Trend', 'Ichimoku_Cloud_Width_Momentum', 'Ichimoku_Price_Position', 'Ichimoku_TK_Cross',
        'MFI_ChangeRate_5d', 'MFI_Divergence',
        'OBV_Breakthrough', 'OBV_ChangeRate_5d', 'OBV_Divergence', 'OBV_Rank', 'OBV_Slope',
        'PVT_Divergence', 'PVT_MA_Deviation', 'PVT_Momentum_Reversal',
        'RVI_Cross', 'RVI_Diff', 'RVI_Strength', 'RVI_Trend', 'RVI_Value', 'RVI_Volume'
    ]
    
    all_factors = fundamental_factors + risk_trading_factors + technical_factors
    
    # Ensure notebooks directory exists
    notebooks_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'notebooks')
    os.makedirs(notebooks_dir, exist_ok=True)
    
    print(f"Generating notebooks for {len(all_factors)} factors in {notebooks_dir}...")
    
    for factor in all_factors:
        filename = f"backtest_{factor}.ipynb"
        filepath = os.path.join(notebooks_dir, filename)
        
        if os.path.exists(filepath):
            print(f"Skipping {filename} (already exists)")
            continue
            
        notebook_content = create_notebook_content(factor)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(notebook_content, f, indent=4)
            
        print(f"Generated {filename}")

if __name__ == "__main__":
    main()
