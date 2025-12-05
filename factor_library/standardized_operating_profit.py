import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class StandardizedOperatingProfit(BaseFactor):
    """
    Standardized Operating Profit (TTM).
    This factor calculates the standardized operating profit using the formula:
    Standardized Operating Profit (TTM) = (OperatingProfit_TTM - mean(OperatingProfit_TTM,t-T:t-1)) / std(OperatingProfit_TTM,t-T:t-1)
    """

    @property
    def name(self) -> str:
        return "standardized_operating_profit"

    @property
    def required_fields(self) -> list:
        return ['op_income']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        T = 6  # Default to 6 quarters for the historical window

        # Calculate TTM Operating Profit
        operating_profit_ttm = df['op_income']

        # Calculate the mean and std for the last T quarters (rolling window)
        rolling_mean = df['op_income'].shift(1).rolling(T).mean()  # Shift to exclude current quarter
        rolling_std = df['op_income'].shift(1).rolling(T).std()  # Shift to exclude current quarter

        # Standardized Operating Profit
        standardized_operating_profit = (operating_profit_ttm - rolling_mean) / rolling_std

        # Ensure numeric
        standardized_operating_profit = pd.to_numeric(standardized_operating_profit, errors='coerce')

        result = pd.DataFrame({
            self.name: standardized_operating_profit,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
