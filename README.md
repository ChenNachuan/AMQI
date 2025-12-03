
# AMQI 2025 Project

## Project Description
This project is for the 2025 Asset Management and Quantitative Investment course. The goal is to build a robust factor investing pipeline using Chinese stock market data.

## Directory Structure

The project follows a modular "Kitchen & Recipe" architecture:

-   **`factor_library/` (The Recipes)**: Contains the core logic for factor calculation.
    -   `base_factor.py`: Abstract base class.
    -   `universe.py`: Universe filtering logic (e.g., Market Cap filter).
    -   **Available Factors**:
        -   `atr.py` & variations: Average True Range, Expansion, Trend, etc.
        -   `beta.py`: Beta factor.
        -   `bollinger.py` & variations: Bollinger Bands, Breakout, Squeeze, etc.
        -   `ichimoku.py` & variations: Ichimoku Cloud, Trend, TK Cross, etc.
        -   `mfi.py` & variations: Money Flow Index, Divergence.
        -   `momentum.py`: Momentum (R11).
        -   `obv.py` & variations: On-Balance Volume, Slope, Rank, etc.
        -   `pvt.py` & variations: Price Volume Trend, Divergence.
        -   `reversal.py`: Reversal factor.
        -   `rvi.py` & variations: Relative Vigor Index, Strength, Cross.
        -   `swma.py`: Sine Weighted Moving Average.
        -   `tema.py`: Triple Exponential Moving Average.
        -   `turnover.py`: Turnover factor.
        -   `volatility.py`: Volatility factor.
        -   **Fundamental Factors**: `ocf_ni.py`, `int_coverage.py`, `tax_rate.py`, `ap_turnover.py`, `fa_turnover.py`, `roe_mom_na_growth.py`, etc.

-   **`backtest/` (The Engine)**: A production-ready backtesting framework.
    -   `config.py`: **[NEW]** Loads global configuration.
    -   `metrics.py`: Statistical core (Newey-West t-stats, Sharpe, etc.).
    -   `analyzer.py`: Logic layer (IC, Sorting, Turnover, Regressions, **Daily Returns**).
    -   `plotting.py`: Visualization tools.
    -   `engine.py`: Facade for easy usage.

-   **`scripts/` (The Kitchen)**: Execution scripts that load data, use recipes, and produce output.
    -   `utils/financial_utils.py`: Core financial logic (TTM conversion, YoY growth).
    -   `utils/generate_adj_prices.py`: **[NEW]** Generates full daily adjusted price history.
    -   `download_ext_data.py`: Downloads external data (Indices, Macro, Adj Factors).
    -   `factors/construct_fundamental_factors.py`: Calculates monthly fundamental factors with **refined TTM logic**.
    -   `factors/construct_technical_factors.py`: Calculates monthly technical factors.
    -   `run_risk_factors.py`: Calculates daily risk factors.
    -   `finalize_dataset.py`: Merges factors, applies filters, and creates the final dataset.
    -   `test_backtest.py`: Verifies the backtest engine.

-   **`data/` (The Pantry)**: Data storage.
    -   `raw_data/`: Raw parquet files from Tushare (Stocks, Indices, Macro).
    -   `data_cleaner/`: Cleaned whitelist and `daily_adj.parquet` (Daily Adjusted Prices).
    -   `factors/`: Intermediate factor datasets.
    -   `final_dataset.parquet`: The final, analysis-ready dataset.
    -   `benchmark.parquet`: Optional benchmark data.

## Configuration

A global configuration file `config.yaml` is available in the project root to control backtest parameters:

```yaml
backtest:
  start_date: '2010-01-01'
  end_date: '2024-12-31'
```

## Setup

1.  **Environment**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Data**:
    Ensure `data/raw_data/` contains the necessary parquet files (`daily.parquet`, `daily_basic.parquet`, etc.).

## Workflow

To reproduce the dataset from scratch:

1.  **Clean Data** (Generate Whitelist & Adjusted Prices):
    ```bash
    python data/data_cleaner/clean_data.py
    python scripts/utils/generate_adj_prices.py  # Required for daily backtest
    ```

0.  **Download Data** (Optional):
    ```bash
    python scripts/download_ext_data.py
    ```

2.  **Construct Factors**:
    ```bash
    # Fundamental Factors (Monthly) - Uses Refined TTM Logic
    python scripts/factors/construct_fundamental_factors.py
    
    # Technical Factors (Monthly)
    python scripts/factors/construct_technical_factors.py

    # Risk Factors (Daily) - Vectorized
    python scripts/run_risk_factors.py
    ```

3.  **Finalize Dataset** (Merge & Filter):
    ```bash
    python scripts/finalize_dataset.py
    ```

4.  **Run Backtest**:
    ```python
    from backtest.engine import BacktestEngine
    import pandas as pd
    
    df = pd.read_parquet('data/final_dataset.parquet')
    engine = BacktestEngine(df, factor_name='beta')
    summary = engine.run_analysis(weighting='vw')
    engine.plot_results()
    ```

## Key Design Decisions

-   **Financial Logic (TTM)**: Fundamental factors use **Trailling Twelve Months (TTM)** logic derived from YTD reports. This is now strictly filtered to apply only to flow variables (e.g., Revenue, Net Income, Cash Flow) to ensure accuracy.
-   **Daily Return Backtesting**: The backtest engine now supports **Daily Return with Monthly Rebalancing**. It calculates the daily performance of monthly-rebalanced portfolios using `daily_adj.parquet`, ensuring continuous and smooth cumulative return plots.
-   **Vectorization**: Risk factors (Beta, Volatility) use optimized matrix operations for high performance.
-   **Frequency**: Fundamental factors are Monthly; Risk factors are Daily. They are aligned (Left Join) in the final step.
-   **Filtering**: A dynamic 30% Market Cap filter is applied during the final assembly to ensure tradability.
-   **Target**: `next_ret` is the future 1-month return ($R_{t+1}$), aligned for predictive modeling.
-   **Backtest Engine**:
    -   **Turnover**: Calculates Q5 portfolio turnover to estimate transaction costs.
    -   **Benchmark Integration**: Supports external benchmark for Active Return and CAPM Alpha.
    -   **Long-Only Metrics**: Evaluates the Top Quantile (Q5) as a standalone product.
