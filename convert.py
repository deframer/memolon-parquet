import argparse
import sys
import zipfile
import tomllib
from pathlib import Path

import pyarrow.csv as pv
import pyarrow.parquet as pq


class Parser:
    """Handles command-line argument parsing."""

    @staticmethod
    def parse_args() -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Extract a TSV from a downloaded ZIP and convert to Parquet."
        )
        parser.add_argument(
            "--type",
            choices=["grouped", "all"],
            required=True,
            help="The type of data to extract (grouped or all)",
        )
        parser.add_argument(
            "--language",
            required=True,
            help="The language code of the TSV file (e.g., 'de', 'en'), or 'all' for all languages",
        )
        parser.add_argument(
            "--version",
            help="The version to append to the output file (defaults to version in pyproject.toml)",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force extraction and conversion even if the output file already exists",
        )
        parser.add_argument(
            "--keeptsv",
            action="store_true",
            help="Keep the intermediate TSV file after conversion",
        )
        return parser.parse_args()


class Unzipper:
    """Handles extraction of files from ZIP archives."""

    def __init__(self, zip_filepath: Path):
        self.zip_filepath = zip_filepath

    def list_tsvs(self) -> list[str]:
        if not self.zip_filepath.exists():
            print(
                f"Error: Could not find '{self.zip_filepath.name}' in '{self.zip_filepath.parent}'."
            )
            print("Please run 'make download' first.")
            sys.exit(1)

        try:
            with zipfile.ZipFile(self.zip_filepath, "r") as zf:
                return [name for name in zf.namelist() if name.endswith(".tsv")]
        except zipfile.BadZipFile:
            print(
                f"Error: '{self.zip_filepath}' is not a valid zip file or is corrupted."
            )
            sys.exit(1)

    def extract(self, internal_filename: str, output_filepath: Path) -> None:
        if not self.zip_filepath.exists():
            print(
                f"Error: Could not find '{self.zip_filepath.name}' in '{self.zip_filepath.parent}'."
            )
            print("Please run 'make download' first.")
            sys.exit(1)

        print(f"Opening '{self.zip_filepath}'...")
        try:
            with zipfile.ZipFile(self.zip_filepath, "r") as zf:
                if internal_filename not in zf.namelist():
                    print(
                        f"Error: '{internal_filename}' not found inside '{self.zip_filepath.name}'."
                    )
                    sys.exit(1)

                print(f"Extracting '{internal_filename}' to '{output_filepath}'...")
                with (
                    zf.open(internal_filename) as source,
                    open(output_filepath, "wb") as target,
                ):
                    target.write(source.read())
        except zipfile.BadZipFile:
            print(
                f"Error: '{self.zip_filepath}' is not a valid zip file or is corrupted."
            )
            sys.exit(1)


class ParquetConverter:
    """Handles conversion from TSV to Parquet format using PyArrow."""

    @staticmethod
    def convert(tsv_filepath: Path, parquet_filepath: Path) -> None:
        print(f"Converting '{tsv_filepath}' to Parquet...")
        try:
            # Our input files are tab-separated
            parse_options = pv.ParseOptions(delimiter="\t")
            table = pv.read_csv(tsv_filepath, parse_options=parse_options)

            # Write to Parquet format
            pq.write_table(table, parquet_filepath)
            print(f"Successfully created Parquet file: '{parquet_filepath}'")
        except Exception as e:
            print(f"Error during Parquet conversion: {e}")
            sys.exit(1)


def main():
    args = Parser.parse_args()

    # Define base paths
    base_dir = Path(__file__).parent.absolute()
    downloads_dir = base_dir / "downloads"
    work_dir = base_dir / "work"

    # Input file configuration
    zip_filename = f"MTL_{args.type}.zip"
    zip_filepath = downloads_dir / zip_filename

    # Ensure work directory exists
    work_dir.mkdir(parents=True, exist_ok=True)

    # Determine version
    version = args.version
    if not version:
        pyproject_path = base_dir / "pyproject.toml"
        if pyproject_path.exists():
            with open(pyproject_path, "rb") as f:
                pyproject_data = tomllib.load(f)
                version = pyproject_data.get("project", {}).get("version", "unknown")
        else:
            version = "unknown"

    unzipper = Unzipper(zip_filepath)

    if args.language.lower() == "all":
        target_tsvs = unzipper.list_tsvs()
        if not target_tsvs:
            print(f"No TSV files found in {zip_filepath}")
            sys.exit(1)
        print(f"Found {len(target_tsvs)} languages in {zip_filename}.")
    else:
        target_tsvs = [f"{args.language}.tsv"]

    for tsv_filename in target_tsvs:
        lang = tsv_filename.replace(".tsv", "")

        # Target output files
        output_tsv = work_dir / f"memolon-{args.type}-{lang}-{version}.tsv"
        output_parquet = work_dir / f"memolon-{args.type}-{lang}-{version}.parquet"

        # Skip logic (checking for parquet output)
        if output_parquet.exists() and not args.force:
            print(
                f"Skipping: '{output_parquet}' already exists. Use --force to overwrite."
            )
            continue

        # 1. Unzip the TSV file
        unzipper.extract(tsv_filename, output_tsv)

        # 2. Convert TSV to Parquet
        ParquetConverter.convert(output_tsv, output_parquet)

        # 3. Cleanup TSV if requested
        if not args.keeptsv:
            print(f"Cleaning up '{output_tsv}'...")
            output_tsv.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
