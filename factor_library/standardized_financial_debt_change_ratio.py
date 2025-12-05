import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class StandardizedFinancialDebtChangeRatio(BaseFactor):
    """
    Standardized Change in Financial Liabilities.
    This factor calculates the change in financial liabilities normalized by average total assets using the formula:
    Standardized Change in Financial Liabilities = (Current Financial Liabilities - Previous Year's Financial Liabilities) / Average Total Assets
    """

    @property
    def name(self) -> str:
        return "standardized_financial_debt_change_ratio"

    @property
    def required_fields(self) -> list:
        return ['st_borr', 'trading_fl', 'notes_payable', 'non_cur_liab_due_1y', 'lt_borr', 'bond_payable',
                'total_assets']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate current financial liabilities
        current_financial_liabilities = (df['st_borr'] + df['trading_fl'] + df['notes_payable'] +
                                         df['non_cur_liab_due_1y'] + df['lt_borr'] + df['bond_payable'])

        # Calculate previous year's financial liabilities
        previous_year_financial_liabilities = (df['st_borr'].shift(4) + df['trading_fl'].shift(4) +
                                               df['notes_payable'].shift(4) + df['non_cur_liab_due_1y'].shift(4) +
                                               df['lt_borr'].shift(4) + df['bond_payable'].shift(4))

        # Calculate average total assets
        avg_total_assets = (df['total_assets'] + df['total_assets'].shift(1)) / 2

        # Standardize the change in financial liabilities
        standardized_change_in_financial_liabilities = (
                                                                   current_financial_liabilities - previous_year_financial_liabilities) / avg_total_assets

        # Ensure numeric
        standardized_change_in_financial_liabilities = pd.to_numeric(standardized_change_in_financial_liabilities,
                                                                     errors='coerce')

        result = pd.DataFrame({
            self.name: standardized_change_in_financial_liabilities,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
