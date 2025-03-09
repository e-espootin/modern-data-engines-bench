from module_spark.spark_test import SparkTest
from module_smallpond.smallpond_test import smallpond_test
from module_duckdb.duckdb_test import DuckDBTest
from module_polars.polars_test import PolarsTest
from module_duckdb.duckdb_inmemory_compare_result import get_compare_result_from_memory
from pathlib import Path
import os
from setuptools._distutils import spawn
import shutil
from utils.logger import setup_logger

logger = setup_logger(__name__)


def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent


def main():

    # Set up project paths
    project_root = get_project_root()
    data_dir = project_root / "data"
    output_dir = project_root / "output"
    print(f"Data directory: {data_dir}")

    # Create directories if they don't exist
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # target test file
    target_test_file = os.getenv(
        'SAMPLE_DATASET', 'sample_transactions_1000k.parquet')
    logger.info(f"Target test file: {target_test_file}")

    # polars
    PolarsTest(data_dir=data_dir, output_dir=output_dir,
               test_file_name=target_test_file).call_Process()

    # duckdb
    DuckDBTest(data_dir=data_dir, output_dir=output_dir,
               test_file_name=target_test_file).call_Process()
    # # spark
    SparkTest(data_dir=data_dir, output_dir=output_dir,
              test_file_name=target_test_file).call_Process()

    # # smallpond
    smallpond_test(data_dir=data_dir, output_dir=output_dir,
                   test_file_name=target_test_file).call_Process()


if __name__ == "__main__":
    main()
