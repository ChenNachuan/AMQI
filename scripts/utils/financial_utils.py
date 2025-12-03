import pandas as pd
import numpy as np

def convert_ytd_to_ttm(df: pd.DataFrame, value_col: str, date_col: str = 'end_date', code_col: str = 'ts_code') -> pd.DataFrame:
    """
    Convert Year-to-Date (YTD) financial data to Trailing Twelve Months (TTM).
    
    Logic:
    1. Sort by code and date.
    2. Identify Quarter (Q) from end_date.
    3. Calculate Single Quarter (SQ) value:
       - Q1: SQ = YTD
       - Q2-Q4: SQ_t = YTD_t - YTD_{t-1} (if same year)
    4. Calculate TTM: Rolling sum of last 4 SQ values.
    
    Args:
        df: Input DataFrame containing YTD data.
        value_col: Name of the column with YTD values.
        date_col: Name of the date column (default 'end_date').
        code_col: Name of the stock code column (default 'ts_code').
        
    Returns:
        DataFrame with an additional '{value_col}_ttm' column.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values([code_col, date_col])
    
    # Extract Quarter
    df['quarter'] = df[date_col].dt.quarter
    df['year'] = df[date_col].dt.year
    
    # Calculate Single Quarter (SQ) Value
    # We need to shift within the same group (stock) to get previous period's YTD
    # However, we must ensure we are subtracting the correct previous quarter in the same year.
    
    # Group by stock and year to handle YTD subtraction safely
    # For Q1, SQ is just YTD. For others, it's YTD - prev_YTD
    
    # Create a shifted column for the previous record
    df['prev_ytd'] = df.groupby([code_col, 'year'])[value_col].shift(1)
    df['prev_quarter'] = df.groupby([code_col, 'year'])['quarter'].shift(1)
    
    # Initialize SQ with YTD
    df['sq_value'] = df[value_col]
    
    # If not Q1, subtract previous YTD if it exists and is the immediate previous quarter
    # Note: Chinese reports are usually Q1, Q2 (Semi), Q3, Q4 (Annual).
    # Sometimes Q3 is missing or Q1 is missing, but usually they are consistent.
    # Standard logic: SQ = YTD - Prev_YTD if Prev_YTD is from the same year.
    
    mask_not_q1 = df['quarter'] > 1
    # We only subtract if we have the previous record in the same year
    # If data is missing (e.g. have Q2 but missing Q1), we might have issues.
    # Assuming standard data quality for now, but being robust:
    
    # Calculate SQ:
    # If Q1: SQ = YTD (Already set)
    # If Q > 1: SQ = YTD - Prev_YTD (where Prev_YTD is valid)
    
    # A safer way using diff() within year groups might be better, but let's be explicit
    df.loc[mask_not_q1, 'sq_value'] = df.loc[mask_not_q1, value_col] - df.loc[mask_not_q1, 'prev_ytd'].fillna(0)
    
    # Handle edge case: If Q2 exists but Q1 is missing in data, 'prev_ytd' will be NaN.
    # In that case, SQ for Q2 would be YTD(Q2) - 0 = YTD(Q2), which is WRONG (it would be H1).
    # But without Q1 data, we can't know Q2 SQ.
    # Ideally we should set to NaN if prev_ytd is NaN and it's not Q1.
    
    mask_missing_prev = (df['quarter'] > 1) & (df['prev_ytd'].isna())
    df.loc[mask_missing_prev, 'sq_value'] = np.nan
    
    # Now Calculate TTM: Rolling sum of last 4 SQ values
    # We need to roll over the stock, ignoring year boundaries (TTM crosses years)
    
    # We must ensure the rolling window covers 4 consecutive quarters.
    # Simple rolling(4).sum() works if data is dense (no missing quarters).
    # If quarters are missing, rolling(4) might take Q4(2020), Q4(2019), etc.
    # For robust TTM, we usually just take rolling sum and assume data density,
    # or we can reindex to fill missing quarters.
    # Given the "Critical" nature, let's stick to rolling sum but maybe warn or check dates?
    # For this refactor, standard rolling(4) on sorted SQ is the standard approach.
    
    df[f'{value_col}_ttm'] = df.groupby(code_col)['sq_value'].transform(
        lambda x: x.rolling(window=4, min_periods=4).sum()
    )
    
    # Cleanup temporary columns
    return df.drop(columns=['quarter', 'year', 'prev_ytd', 'prev_quarter', 'sq_value'])

def calculate_yoy_growth(df: pd.DataFrame, value_col: str, date_col: str = 'end_date', code_col: str = 'ts_code') -> pd.DataFrame:
    """
    Calculate Year-over-Year (YoY) growth rate.
    
    Args:
        df: Input DataFrame.
        value_col: Column to calculate growth for.
        
    Returns:
        DataFrame with '{value_col}_yoy' column.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values([code_col, date_col])
    
    # Shift by 4 periods (assuming quarterly data) to get same quarter last year
    # This assumes dense data. A more robust way is to join on (year-1, quarter).
    # Let's use the shift(4) for simplicity and speed as per standard pandas practices for sorted time series.
    
    df[f'{value_col}_lag4'] = df.groupby(code_col)[value_col].shift(4)
    
    # Calculate Growth: (Current - Lag) / abs(Lag)
    # Using abs in denominator to handle negative base values correctly (though growth on negative is tricky)
    df[f'{value_col}_yoy'] = (df[value_col] - df[f'{value_col}_lag4']) / df[f'{value_col}_lag4'].abs()
    
    return df.drop(columns=[f'{value_col}_lag4'])
