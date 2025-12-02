
# AMQI 2025 Project

## Project Description
This project is for the 2025 Asset Management and Quantitative Investment course. The goal is to build a robust factor investing pipeline using Chinese stock market data.

## Directory Structure

The project follows a modular "Kitchen & Recipe" architecture:

-   **`factor_library/` (The Recipes)**: Contains the core logic for factor calculation.
    -   `base_factor.py`: Abstract base class.
    -   `momentum.py`, `volatility.py`, `beta.py`, etc.: Individual factor definitions.
    -   `universe.py`: Universe filtering logic (e.g., Market Cap filter).

-   **`backtest/` (The Engine)**: A production-ready backtesting framework.
    -   `metrics.py`: Statistical core (Newey-West t-stats, Sharpe, etc.).
    -   `analyzer.py`: Logic layer (IC, Sorting, Turnover, Regressions).
    -   `plotting.py`: Visualization tools.
    -   `engine.py`: Facade for easy usage.

-   **`scripts/` (The Kitchen)**: Execution scripts that load data, use recipes, and produce output.
    -   `construct_fundamental_factors.py`: Calculates monthly fundamental factors.
    -   `run_risk_factors.py`: Calculates daily risk factors.
    -   `finalize_dataset.py`: Merges factors, applies filters, and creates the final dataset.
    -   `test_backtest.py`: Verifies the backtest engine.
    -   `../data/data_loader/download_ext_data.py`: Downloads external data (Indices, Macro).

-   **`data/` (The Pantry)**: Data storage.
    -   `raw_data/`: Raw parquet files from Tushare (Stocks, Indices, Macro).
    -   `data_cleaner/`: Cleaned whitelist (`daily_basic_cleaned.parquet`).
    -   `factors/`: Intermediate factor datasets (`fundamental_factors.parquet`, `risk_factors.parquet`).
    -   `final_dataset.parquet`: The final, analysis-ready dataset.
    -   `benchmark.parquet`: Optional benchmark data.

## Setup

1.  **Environment**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Data**:
    Ensure `data/raw_data/` contains the necessary parquet files (`daily.parquet`, `daily_basic.parquet`, etc.).

## Workflow

To reproduce the dataset from scratch:

1.  **Clean Data** (Generate Whitelist):
    ```bash
    python data/data_cleaner/clean_data.py
    ```

0.  **Download Data** (Optional):
    ```bash
    python data/data_loader/download_ext_data.py
    ```

2.  **Construct Factors**:
    ```bash
    # Fundamental Factors (Monthly)
    python scripts/construct_fundamental_factors.py
    
    # Risk Factors (Daily)
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

-   **Frequency**: Fundamental factors are Monthly; Risk factors are Daily. They are aligned (Left Join) in the final step.
-   **Filtering**: A dynamic 30% Market Cap filter is applied during the final assembly to ensure tradability.
-   **Target**: `next_ret` is the future 1-month return ($R_{t+1}$), aligned for predictive modeling.
-   **Backtest Engine**:
    -   **Turnover**: Calculates Q5 portfolio turnover to estimate transaction costs.
    -   **Benchmark Integration**: Supports external benchmark for Active Return and CAPM Alpha.
    -   **Long-Only Metrics**: Evaluates the Top Quantile (Q5) as a standalone product.
