from duckdb import duckdb
from pathlib import Path
import pandas
from utils.logger import setup_logger

logger = setup_logger(__name__)


def get_project_root() -> str:
    """Returns project root folder."""
    db_path = f"{Path(__file__).parent.parent.parent}/.dbdir"
    Path(db_path).mkdir(parents=True, exist_ok=True)
    return f"{db_path}/duckdb_results.db"


def persist_compare_result_into_memory(compare_result: list):
    try:
        with duckdb.connect(get_project_root()) as con:
            con.execute(
                "CREATE TABLE if not exists compare_result (process_engine VARCHAR(100), process_type varchar(100), time_duration VARCHAR(20), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            # Insert the data into the table
            if compare_result:
                con.execute(
                    f"INSERT INTO compare_result(process_engine, process_type, time_duration) VALUES ('{compare_result[0]}', '{compare_result[1]}', '{compare_result[2]}')")

    except Exception as e:
        logger.error(
            f"Error in persist_compare_result_into_memory: {e}")
        raise e


def get_compare_result_from_memory() -> pandas.DataFrame:
    try:
        logger.info("Getting compare result from memory")
        with duckdb.connect(get_project_root()) as con:
            # Check if the table exists first
            table_exists = con.execute(
                "show tables;").fetchone()
            if not table_exists:
                return None
            # Query the data
            result = con.execute(
                "SELECT * FROM compare_result").df()
            return result
    except Exception as e:
        logger.error(
            f"Error in get_compare_result_from_memory: {e}")
        raise e
