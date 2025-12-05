import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class SalesExpenseRatio(BaseFactor):
    """
    Sales Expense Ratio.
    This factor calculates the ratio of Sales Expenses to Revenue for the latest 12 months (TTM).
    The formula is as follows:
    Sales Expense Ratio = Sales Expenses_TTM / Revenue_TTM
    """

    @property
    def name(self) -> str:
        return "sales_expense_ratio"

    @property
    def required_fields(self) -> list:
        return ['sell_exp', 'revenue']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate Sales Expense Ratio
        sales_expense_ratio = df['sell_exp'] / df['revenue']

        # Ensure numeric
        sales_expense_ratio = pd.to_numeric(sales_expense_ratio, errors='coerce')

        result = pd.DataFrame({
            self.name: sales_expense_ratio,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
