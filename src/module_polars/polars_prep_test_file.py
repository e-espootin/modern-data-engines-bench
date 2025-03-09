import polars as pl
import random
from pathlib import Path


class ParquetProcessor:
    def __init__(self):
        self.input_path = f"{self.get_project_root().parent}/data/sample_transactions_100000k.parquet"
        self.output_path = f"{self.get_project_root().parent}"

    def get_project_root(self) -> Path:
        """Returns project root folder."""
        return Path(__file__).parent.parent

    def read_parquet(self):
        """Read the parquet file into a polars DataFrame"""
        try:
            df = pl.read_parquet(self.input_path)
            print(f"Successfully read {len(df)} rows from {self.input_path}")
            return df
        except Exception as e:
            print(f"Error reading parquet file: {e}")
            return None

    def read_schema(self):
        """Read the schema of the parquet file"""
        try:
            df = pl.read_parquet(self.input_path)
            print(f"Schema of {self.input_path} is {df.schema}")
            print(df.describe())
            return df.schema
        except Exception as e:
            print(f"Error reading parquet file: {e}")

    def create_sample(self, df, sample_size=1):
        """Create a small sample from the DataFrame and save it as a parquet file"""
        if df is None or len(df) == 0:
            print("No data to sample")
            return None

        # Take a random sample
        if len(df) <= sample_size * 1000:
            sample_df = df
            print(f"Dataset has only {len(df)} rows, using entire dataset")
        else:
            # Using polars built-in sampling
            sample_df = df.sample(sample_size * 1000, shuffle=True)
            print(f"Created sample with {str(sample_size * 1000)} rows")

        # Save sample to parquet
        try:
            output_path = f"{self.output_path}/data/sample_transactions_{str(sample_size)}k.parquet"
            sample_df.write_parquet(output_path, compression="snappy")
            print(f"Successfully saved sample to {output_path}")
            return sample_df
        except Exception as e:
            print(f"Error saving sample: {e}")
            return None


if __name__ == "__main__":
    processor = ParquetProcessor()
    df = processor.read_parquet()
    if df is not None:
        processor.create_sample(df, sample_size=30000)
    # df = processor.read_schema()
    # df.collect()
