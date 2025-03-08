import time
import functools
from module_duckdb.duckdb_inmemory_compare_result import persist_compare_result_into_memory
from .logger import setup_logger

logger = setup_logger(__name__)


def log_execution_time(process_engine: str = 'default'):
    def log_execution_time_inner(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(
                f"### Time Duration: Function {func.__name__} took {execution_time:.4f} seconds to execute")
            # persist the result into memory with process engine name
            persist_compare_result_into_memory(
                [process_engine, func.__name__, execution_time])
            return result
        return wrapper
    return log_execution_time_inner
