
import pandas as pd
from pathlib import Path
import sys

def convert_dta_to_parquet(input_dir: Path, output_dir: Path):
    """
    Converts all .dta files in the input directory to .parquet format in the output directory.
    """
    if not input_dir.exists():
        print(f"Error: Input directory {input_dir} does not exist.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    dta_files = list(input_dir.glob("*.dta"))
    if not dta_files:
        print(f"No .dta files found in {input_dir}")
        return

    for dta_file in dta_files:
        try:
            print(f"Converting {dta_file.name}...")
            df = pd.read_stata(dta_file)
            output_file = output_dir / dta_file.with_suffix(".parquet").name
            df.to_parquet(output_file, index=False)
            print(f"Saved to {output_file}")
        except Exception as e:
            print(f"Failed to convert {dta_file.name}: {e}")

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    data_dir = base_dir / "data"
    convert_dta_to_parquet(data_dir, data_dir)
