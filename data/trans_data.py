"""Utility to convert Stata .dta files into Python-friendly formats."""

from pathlib import Path
import argparse
import pandas as pd


def convert_dta(input_path: Path, output_path: Path | None, file_format: str) -> Path:
	"""Read a .dta file and persist it in the requested format."""
	df = pd.read_stata(input_path)

	if file_format == "parquet":
		output_path = output_path or input_path.with_suffix(".parquet")
		df.to_parquet(output_path, index=False)
	else:
		output_path = output_path or input_path.with_suffix(".csv")
		df.to_csv(output_path, index=False)

	return output_path


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("input", type=Path, help="Path to the source .dta file")
	parser.add_argument(
		"--output",
		type=Path,
		help="Optional output path. Defaults to the input name with a new suffix.",
	)
	parser.add_argument(
		"--format",
		choices=("parquet", "csv"),
		default="parquet",
		help="Target format. Parquet keeps dtypes and is recommended.",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	output_path = convert_dta(args.input, args.output, args.format)
	print(f"Saved {output_path}")


if __name__ == "__main__":
	main()
