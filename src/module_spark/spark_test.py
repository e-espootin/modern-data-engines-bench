from .config import SparkConfig
from pyspark.sql.functions import sum, broadcast
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
            # self.save_results_spark()
        except Exception as e:
            logger.error(f"Error in call_Process: {e}")
            raise e

    @log_execution_time(process_engine="spark")
    def load_data_spark(self):
        try:
            # read the test data
            filename = f"{self.data_dir}/{self.test_file_name}"
            logger.debug(f"Loading data from {filename}")
            # load all data
            # self.df = self.spark.read.parquet(filename)

            #! [Optimization] filter columns on read
            self.df = self.spark.read.parquet(
                filename).select("Cust_ID", "Amount", "Month")
        except Exception as e:
            logger.error(f"Error in _load_data_spark: {e}")
            raise e

    @log_execution_time(process_engine="spark")
    def process_data_spark(self):
        try:
            '''simple join, AQE is enabled'''
            df_agg = self.df.groupBy("Cust_ID").agg(
                sum("Amount").alias("total_amount"))
            # get some aggregate values >> transaction volumes per month
            df_agg_month = self.df.groupBy("Month").agg(
                sum("Amount").alias("Month_total_Amount"))
            # concat the data
            self.transformed_df = self.df.join(df_agg, "Cust_ID", "inner").join(
                df_agg_month, "Month", "inner")

            '''bucketed'''
            # self.df.write.bucketBy(10, 'Cust_id').sortBy(
            #     'Cust_ID').saveAsTable('df')
            # df_agg.write.bucketBy(10, 'Cust_ID').sortBy(
            #     'Cust_ID').saveAsTable('df_agg')
            # df_agg_month.write.bucketBy(10, 'Month').sortBy(
            #     'Month').saveAsTable('df_agg_month')

            # df_bucketed = self.spark.table("df")
            # df_agg_bucketed = self.spark.table("df_agg")
            # df_agg_month_bucketed = self.spark.table("df_agg_month")

            # self.transformed_df = df_bucketed.join(df_agg_bucketed, "Cust_ID", "inner").join(
            #     broadcast(df_agg_month_bucketed), "Month", "inner")

            logger.info(f"explain is {self.transformed_df.explain(mode="extended")}")
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
