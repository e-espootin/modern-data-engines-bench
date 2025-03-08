import logging
import sys
import colorlog


def setup_logger(name: str = __name__, level=logging.INFO):
    """Set up a colored logger instance"""
    logger = logging.getLogger(name)

    if not logger.handlers:
        # Create color formatter
        formatter = colorlog.ColoredFormatter(
            fmt="%(log_color)s%(asctime)s | %(levelname)-8s | %(message)s%(reset)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white"
            },
            secondary_log_colors={},
            style="%",
        )

        # Create console handler
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(formatter)

        # Add handler to logger
        logger.setLevel(level)
        logger.addHandler(handler)

    return logger
