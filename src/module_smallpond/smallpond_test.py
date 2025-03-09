from setuptools._distutils import spawn
import smallpond
from pathlib import Path
from utils.log_decorator import log_execution_time
from utils.logger import setup_logger
import shutil

logger = setup_logger(__name__)


class smallpond_test:
    def __init__(self, data_dir: str, output_dir: str, test_file_name: str):
        self.test_file_name = test_file_name
        self.data_dir = data_dir
        self.output_dir = output_dir
        try:

            # Initialize smallpond
            self.sp = smallpond.init()
            #! [optimized]
            # args = {
            #     "data_root": f"{self.output_dir}",
            #     "duckdb_path": str(Path(output_dir) / "smallpond_duckdb.db"),
            #     "memory_per_worker": "4g",  # Increase memory per worker
            #     "object_store_memory": "8g",  # Larger object store
            #     "num_cpus": 2,  # Control CPU usage
            #     # Add spill directory
            #     "spill_dir": str(Path(output_dir) / "smallpond_spill")
            # }
            # self.sp = smallpond.init(**args)
            self.df = None
            self.transformed_df = None
        except Exception as e:
            logger.error(f"Error in smallpond_test: {e}")
            raise e

    def call_Process(self):
        '''call the process_data method'''
        try:
            # load data
            self.load_data()

            # process data
            self.process_data()

            # save results
            # self.save_results()
        except Exception as e:
            logger.error(f"Error in call_Process: {e}")
            raise e

    @log_execution_time(process_engine="smallpond")
    def load_data(self):
        '''load data from parquet file'''
        try:
            filename = f"{self.data_dir}/{self.test_file_name}"
            logger.info(f"Loading data from {filename}")
            self.df = self.sp.read_parquet(filename
                                           )
            logger.info(f"Data loaded from {self.df.to_pandas().head(1)}")
        except Exception as e:
            logger.error(f"Error in _load_data: {e}")
            raise e

    def get_table_schema(self):
        return "{'CUST_ID': String, 'START_DATE': String, 'END_DATE': String, 'TRANS_ID': String, 'DATE': String, 'YEAR': Int32, 'MONTH': Int32, 'DAY': Int32, 'EXP_TYPE': String, 'AMOUNT': Float64}"

    @log_execution_time(process_engine="smallpond")
    def process_data(self):
        try:
            # transform data
            self.df = self.df.repartition(3, hash_by="MONTH")

            df_agg_cust_id = self.sp.partial_sql(
                "SELECT CUST_ID, sum(AMOUNT) sum_AMOUNT FROM {0} GROUP BY CUST_ID", self.df)
            df_agg_month = self.sp.partial_sql(
                "SELECT MONTH, sum(AMOUNT) month_total_AMOUNT FROM {0} GROUP BY MONTH", self.df)
            # join data
            self.transformed_df = self.sp.partial_sql(
                "SELECT trans.*, c.sum_AMOUNT, m.month_total_AMOUNT  \
                    FROM {0} as trans \
                    inner join {1} as c \
                        on trans.CUST_ID = c.CUST_ID \
                    inner join {2} as m \
                        on trans.MONTH = m.MONTH \
                          ", self.df, df_agg_cust_id, df_agg_month)

            # repartition data
            self.transformed_df = self.transformed_df.repartition(
                3, hash_by="MONTH")

        except Exception as e:
            logger.error(f"Error in process_data: {e}")
            raise e

    @log_execution_time(process_engine="smallpond")
    def save_results(self):
        try:
            files_path = f"{self.output_dir}/smallpond_output/"
            logger.info(f"Saving results to {files_path}")
            # clean up path
            if Path(files_path).exists():
                shutil.rmtree(files_path)
            Path(
                f"{self.output_dir}/smallpond_output/").mkdir(parents=True, exist_ok=True)

            self.transformed_df.write_parquet(files_path)
        except Exception as e:
            logger.error(f"Error in save_results: {e}")
            raise e
