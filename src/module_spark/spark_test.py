from .config import SparkConfig
from pyspark.sql.functions import sum
from utils.log_decorator import log_execution_time
from utils.logger import setup_logger

logger = setup_logger(__name__)


class SparkTest:
    def __init__(self, data_dir: str, output_dir: str, test_file_name: str):
        self.test_file_name = test_file_name
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.spark = SparkConfig().get_spark_session()
        self.df = None
        self.transformed_df = None

    def call_Process(self):
        '''call the process_data method'''
        try:
            # load data
            self.load_data_spark()

            # process data
            self.process_data_spark()

            # save results
            self.save_results_spark()
        except Exception as e:
            logger.error(f"Error in call_Process: {e}")
            raise e

    @log_execution_time(process_engine="spark")
    def load_data_spark(self):
        try:
            # read the test data
            filename = f"{self.data_dir}/{self.test_file_name}"
            logger.debug(f"Loading data from {filename}")
            self.df = self.spark.read.parquet(filename)
        except Exception as e:
            logger.error(f"Error in _load_data_spark: {e}")
            raise e

    @log_execution_time(process_engine="spark")
    def process_data_spark(self):
        try:
            # transform the data
            # logger.debug(self.df.show(1))
            # get some aggregate values >> get overall sum of Amount for each customer
            df_agg = self.df.groupBy("Cust_ID").agg(
                sum("Amount").alias("total_amount"))
            # logger.debug(df_agg.show(1))
            # get some aggregate values >> transaction volumes per month
            df_agg_month = self.df.groupBy("Month").agg(
                sum("Amount").alias("Month_total_Amount"))
            # logger.debug(df_agg_month.show(1))
            # concat the data
            self.transformed_df = self.df.join(df_agg, "Cust_ID", "inner").join(
                df_agg_month, "Month", "inner")
            logger.debug(self.transformed_df.show(1))
        except Exception as e:
            logger.error(f"Error in process_data_spark: {e}")
            raise e

    @log_execution_time(process_engine="spark")
    def save_results_spark(self):
        try:
            # write the data
            output_file = f"{self.output_dir}/spark_output/spark"
            logger.debug(f"Writing data to {output_file}")
            self.transformed_df.write.parquet(output_file, mode="overwrite")
        except Exception as e:
            logger.error(f"Error in save_results_spark: {e}")
            raise e
