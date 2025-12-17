#!/bin/bash
set -e

# Data Update Script
# 1. Downloads all data for range 20230101-20251215
# 2. Constructs all factors
# 3. Finalizes dataset (Weekly Frequency)

echo "=== Starting Data Update Pipeline ==="

echo "[1/3] Downloading Raw Data..."
echo "  - Downloading Daily Data..."
python data/data_loader/download_daily_data.py
echo "  - Downloading Balance Sheet..."
python data/data_loader/download_balancesheet.py
echo "  - Downloading Income Statement..."
python data/data_loader/download_income.py
echo "  - Downloading Cash Flow..."
python data/data_loader/download_cashflow.py
echo "  - Downloading Financial Indicators..."
python data/data_loader/download_fina_indicator.py
echo "  - Downloading Dividends..."
python data/data_loader/download_dividend.py
echo "  - Downloading Extended Data (Macro/Index)..."
python data/data_loader/download_ext_data.py
echo "  - Downloading Index Monthly..."
python data/data_loader/index.py

echo "[2/3] Constructing Factors..."
echo "  - Constructing Fundamental Factors..."
python scripts/factors/construct_fundamental_factors.py
echo "  - Constructing Risk Factors..."
python scripts/factors/construct_risk_factors.py
echo "  - Constructing Technical Factors..."
python scripts/factors/construct_technical_factors.py

echo "[3/3] Finalizing Dataset (Weekly)..."
python scripts/utils/finalize_dataset.py

echo "=== All Tasks Completed Successfully ==="
