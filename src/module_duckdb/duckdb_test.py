import duckdb
import pandas as pd
from pathlib import Path
from utils.log_decorator import log_execution_time
from utils.logger import setup_logger

logger = setup_logger(__name__)


class DuckDBTest:
    def __init__(self, data_dir: str, output_dir: str, test_file_name: str):
        self.test_file_name = test_file_name
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.con = duckdb.connect(f"{self.output_dir}/duckdb_processing.db")
        self.df = None
        self.transformed_df = None

    def call_Process(self):
        '''call the process_data method'''
        try:
            # load data
            self.load_data_duckdb()

            # process data
            self.process_data_duckdb()

            # save results
            # self.save_results_duckdb()
        except Exception as e:
            logger.error(f"Error in call_Process: {e}")
            raise e

    @log_execution_time(process_engine="duckdb")
    def load_data_duckdb(self):
        try:
            # read the test data
            filename = f"{self.data_dir}/{self.test_file_name}"
            logger.debug(f"Loading data from {filename}")

            # [before optimization]load data into parquet
            # self.con.execute(
            #     f"CREATE OR REPLACE Table source_data AS SELECT * FROM read_parquet('{filename}')")

            #! [Optimization] read multithread
            self.con.execute("PRAGMA threads=4")
            self.con.execute(
                f"CREATE OR REPLACE Table source_data AS SELECT CUST_ID, MONTH, AMOUNT FROM read_parquet('{filename}')")
            #! [Optimization] table DDL and data types
            # PRAGMA table_info('source_data');

            logger.debug("Data loaded successfully")
        except Exception as e:
            logger.error(f"Error in _load_data_spark: {e}")
            raise e

    @log_execution_time(process_engine="duckdb")
    def process_data_duckdb(self):
        try:
            #! [Optimization] create index
            self.con.execute(
                "CREATE INDEX IF NOT EXISTS idx_cust_id ON source_data(Cust_ID)")
            self.con.execute(
                "CREATE INDEX IF NOT EXISTS idx_month ON source_data(MONTH)")
            #! [Optimization] EXPLAIN or EXPLAIN ANALYZE
            # transform the data
            self.transformed_df = self.con.execute("""
                with ct_agg_customer as (
                    select
                        Cust_ID,
                        sum(amount) as total_amount
                    from source_data
                    group by Cust_ID
                ),
                ct_agg_month as (
                    select
                        Month,
                        sum(Amount) as Month_total_Amount
                    from source_data
                    group by Month
                )
                select
                    sd.*,
                    c.total_amount,
                    m.Month_total_Amount
                from source_data sd
                inner join ct_agg_customer as c
                    on sd.Cust_ID = c.Cust_ID
                inner join ct_agg_month as m
                    on sd.Month = m.Month
                             
            """).df()
        except Exception as e:
            logger.error(f"Error in process_data_spark: {e}")
            raise e

    @log_execution_time(process_engine="duckdb")
    def save_results_duckdb(self):
        try:
            # write the data
            output_file = f"{self.output_dir}/duckdb_output/duckdb_data_output.parquet"
            logger.debug(f"Writing data to {output_file}")
            Path(f"{self.output_dir}/duckdb_output/").mkdir(parents=True, exist_ok=True)
            self.transformed_df.to_parquet(output_file)
        except Exception as e:
            logger.error(f"Error in save_results_spark: {e}")
            raise e
