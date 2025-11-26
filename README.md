
# AMQI 2025 Project

## Project Description
This project is for the 2025 Asset Management and Quantitative Investment course. The goal is to build a robust factor investing pipeline using Chinese stock market data.

## Directory Structure

The project follows a modular "Kitchen & Recipe" architecture:

-   **`factor_library/` (The Recipes)**: Contains the core logic for factor calculation.
    -   `base_factor.py`: Abstract base class.
    -   `momentum.py`, `volatility.py`, `beta.py`, etc.: Individual factor definitions.
    -   `universe.py`: Universe filtering logic (e.g., Market Cap filter).

-   **`scripts/` (The Kitchen)**: Execution scripts that load data, use recipes, and produce output.
    -   `construct_fundamental_factors.py`: Calculates monthly fundamental factors.
    -   `run_risk_factors.py`: Calculates daily risk factors.
    -   `finalize_dataset.py`: Merges factors, applies filters, and creates the final dataset.

-   **`data/` (The Pantry)**: Data storage.
    -   `raw_data/`: Raw parquet files from Tushare.
    -   `data_cleaner/`: Cleaned whitelist (`daily_basic_cleaned.parquet`).
    -   `factors/`: Intermediate factor datasets (`fundamental_factors.parquet`, `risk_factors.parquet`).
    -   `final_dataset.parquet`: The final, analysis-ready dataset.

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

## Usage

Load the final dataset for analysis:

```python
import pandas as pd
df = pd.read_parquet('data/final_dataset.parquet')
print(df.head())
```

## Key Design Decisions

-   **Frequency**: Fundamental factors are Monthly; Risk factors are Daily. They are aligned (Left Join) in the final step.
-   **Filtering**: A dynamic 30% Market Cap filter is applied during the final assembly to ensure tradability.
-   **Target**: `next_ret` is the future 1-month return ($R_{t+1}$), aligned for predictive modeling.
