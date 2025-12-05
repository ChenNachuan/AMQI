import pandas as pd
import numpy as np
from .base_factor import BaseFactor


class QuarterlyAbnormalGm(BaseFactor):
    """
    Quarterly Abnormal Gross Margin.
    This factor calculates the abnormal gross margin for the latest quarter.
    The formula is as follows:
    (GP_q - (GP_q-4 * CS_q / CS_q-4)) / TA_q
    """

    @property
    def name(self) -> str:
        return "quarterly_abnormal_gm"

    @property
    def required_fields(self) -> list:
        return ['revenue', 'oper_cost', 'c_fr_sale_sg', 'total_assets']

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dependencies(df)

        # Calculate Gross Profit (GP) as Revenue - Operating Cost
        gp = df['revenue'] - df['oper_cost']

        # Calculate Sales (CS) for growth adjustment
        cs = df['c_fr_sale_sg']

        # Calculate the adjusted GP (GP_q-4 * CS_q / CS_q-4)
        adjusted_gp = gp.shift(4) * (cs / cs.shift(4))

        # Calculate Quarterly Abnormal Gross Margin
        abnormal_gm = (gp - adjusted_gp) / df['total_assets']

        # Ensure numeric
        abnormal_gm = pd.to_numeric(abnormal_gm, errors='coerce')

        result = pd.DataFrame({
            self.name: abnormal_gm,
            'ts_code': df['ts_code'],
            'end_date': df['end_date']
        })

        if 'ann_date' in df.columns:
            result['ann_date'] = df['ann_date']

        return result
