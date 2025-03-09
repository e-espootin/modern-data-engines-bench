from pyspark.sql import SparkSession
import os
import sys
from utils.log_decorator import log_execution_time
from utils.logger import setup_logger


logger = setup_logger(__name__)


class SparkConfig:
    def __init__(self, app_name="SmallPond Comparison"):
        self.app_name = app_name
        self._spark = None
        #
        self.set_spark_home()

    def set_spark_home(self):
        os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"
        os.environ["SPARK_HOME"] = "/opt/spark/"
        os.environ['PYSPARK_PYTHON'] = sys.executable

    @log_execution_time(process_engine="spark")
    def get_spark_session(self):
        try:
            if not self._spark:
                builder = (SparkSession.builder
                           .appName(self.app_name)
                           .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                           .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                           #    .config("spark.driver.memory", "1g")
                           #    .config("spark.driver.memory", "1g")
                           #    .config("spark.executor.memory", "1g")
                           #    .config("spark.memory.offHeap.enabled", "true")
                           #    .config("spark.memory.offHeap.size", "1g")
                           #    .config("spark.driver.cores", "2")
                           #    .config("spark.executor.cores", "2")
                           #    .config("spark.task.cpus", "1")
                           #    .config("spark.sql.files.maxPartitionBytes", "134217728")
                           .config("spark.sql.shuffle.partitions", "200")
                           .config("spark.io.compression.codec", "snappy")
                           #    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                           #    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                           #    .config("spark.default.parallelism", "4")
                           )

                # Use local[*] by default if no master is specified
                if "SPARK_MASTER" in os.environ:
                    builder = builder.master(os.environ["SPARK_MASTER"])
                else:
                    builder = builder.master("local[*]")

                self._spark = builder.getOrCreate()
                logger.info(
                    f"Created Spark session with app name: {self.app_name}")

            return self._spark
        except Exception as e:
            logger.error(f"Error in get_spark_session: {e}")
            raise e

    def close_session(self):
        if self._spark:
            self._spark.stop()
            self._spark = None
