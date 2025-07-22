import os
import datetime
from pytz import timezone
import logging
from logging.handlers import RotatingFileHandler

# Set timezone to Eastern for logs
logging.Formatter.converter = lambda *args: datetime.datetime.now(tz=timezone('US/Eastern')).timetuple()

# Define a module-level variable to store the logger
_logger = None

def get_logger() -> logging.Logger:
    global _logger
    if _logger is not None:
        return _logger

    logger = logging.getLogger("main_logger")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if this gets called multiple times
    if not logger.handlers:
        log_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),'desired_log_name.log') 
        #   root            src             package_name
        handler = RotatingFileHandler(log_file_path, maxBytes=1024*1024, backupCount=3)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    _logger = logger
    return logger