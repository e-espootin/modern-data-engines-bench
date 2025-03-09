import polars as pl
from pathlib import Path
from utils.log_decorator import log_execution_time
from utils.logger import setup_logger

logger = setup_logger(__name__)


class PolarsTest:
    def __init__(self, data_dir: str, output_dir: str, test_file_name: str):
        self.test_file_name = test_file_name
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.df = None
        self.transformed_df = None

    def call_Process(self):
        '''call the process_data method'''
        try:
            # load data
            self.load_data_polars()

            # process data
            self.process_data_polars()

            # save results
            # self.save_results_polars()
        except Exception as e:
            logger.error(f"Error in call_Process: {e}")
            raise e

    @log_execution_time(process_engine="polars")
    def load_data_polars(self):
        try:
            # read the test data
            filename = f"{self.data_dir}/{self.test_file_name}"
            logger.info(f"Loading data from {filename}")

            # load all data
            # self.df = pl.read_parquet(filename, use_pyarrow=True)

            #! [Optimization] Use multithreading and adjust column selection if needed
            self.df = pl.scan_parquet(filename).collect(
                streaming=True,
                use_pyarrow=True,
                parallel=True
            )
        except Exception as e:
            logger.error(f"Error in _load_data_polars: {e}")
            raise e

    @log_execution_time(process_engine="polars")
    def process_data_polars(self):
        try:
            # transform the data
            # get some aggregate values >> get overall sum of Amount for each customer
            df_agg = self.df.group_by("CUST_ID").agg(
                pl.sum("AMOUNT").alias("total_amount"))
            # logger.debug(df_agg.head(1))

            # get some aggregate values >> transaction volumes per month
            df_agg_month = self.df.group_by("MONTH").agg(
                pl.sum("AMOUNT").alias("Month_total_Amount"))
            # logger.debug(df_agg_month.head(1))

            # concat the data
            self.transformed_df = self.df.join(
                df_agg, on="CUST_ID", how="inner"
            ).join(
                df_agg_month, on="MONTH", how="inner"
            )

            #! [Optimization]
            # # TODO
            logger.debug(self.transformed_df.head(1))
        except Exception as e:
            logger.error(f"Error in process_data_polars: {e}")
            raise e

    @log_execution_time(process_engine="polars")
    def save_results_polars(self):
        try:
            # write the data
            output_file = f"{self.output_dir}/polars_output/polars_output.parquet"
            logger.debug(f"Writing data to {output_file}")
            Path(f"{self.output_dir}/polars_output").mkdir(parents=True, exist_ok=True)
            self.transformed_df.write_parquet(output_file)
        except Exception as e:
            logger.error(f"Error in save_results_polars: {e}")
            raise e
