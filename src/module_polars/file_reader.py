import polars as pl
import random


class FileReader:
    def __init__(self, input_path="/Users/ebrahimespootin/dev/publish/smallpond_spark_compare/comparison_project/output/smallpond_output/SqlEngineTask.000005-0afc6a3b-d3ab-42f9-b6f2-9ec604e9eea4.000034.0.0-0.0.0.parquet"):
        self.input_path = input_path

    def read_parquet(self):
        """Read the parquet file into a polars DataFrame"""
        try:
            df = pl.read_parquet(self.input_path)
            print(f"Successfully read {len(df)} rows from {self.input_path}")
            return df.to_pandas().head(5)
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


if __name__ == "__main__":
    processor = FileReader()
    df = processor.read_parquet()
    sch = processor.read_schema()
    print(df)
