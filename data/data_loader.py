
import pandas as pd
from pathlib import Path

def load_stock_data(data_dir: Path = None, filename: str = "csmar_0110sample.parquet") -> pd.DataFrame:
    """
    Loads the processed stock data from a Parquet file.

    Args:
        data_dir (Path, optional): Directory containing the data. Defaults to 'data' relative to this script.
        filename (str, optional): Name of the parquet file. Defaults to "csmar_0110sample.parquet".

    Returns:
        pd.DataFrame: The loaded data.
    """
    if data_dir is None:
        data_dir = Path(__file__).parent
    
    file_path = data_dir / filename
    
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found at {file_path}. Please run convert_data.py first.")
    
    return pd.read_parquet(file_path)

if __name__ == "__main__":
    # Test loading
    try:
        df = load_stock_data()
        print("Data loaded successfully!")
        print(df.head())
        print(df.info())
    except Exception as e:
        print(f"Error loading data: {e}")
