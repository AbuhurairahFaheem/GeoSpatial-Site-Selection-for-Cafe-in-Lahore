# utils/logging.py

import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
LOG_DIR = "logs"
LOG_FILE_NAME = f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

LOG_LEVEL = logging.INFO   # change to DEBUG for development

MAX_LOG_SIZE = 5 * 1024 * 1024   # 5 MB
BACKUP_COUNT = 3                 # keep last 3 logs


# -----------------------------
# CREATE LOG DIRECTORY
# -----------------------------
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


# -----------------------------
# FORMATTER
# -----------------------------
LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | "
    "%(filename)s:%(lineno)d | %(message)s"
)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# -----------------------------
# LOGGER FACTORY
# -----------------------------
def get_logger(name: str) -> logging.Logger:
    """
    Create or retrieve a configured logger
    """

    logger = logging.getLogger(name)

    # Prevent duplicate handlers
    if logger.hasHandlers():
        return logger

    logger.setLevel(LOG_LEVEL)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # -----------------------------
    # CONSOLE HANDLER
    # -----------------------------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # -----------------------------
    # FILE HANDLER (ROTATING)
    # -----------------------------
    file_path = os.path.join(LOG_DIR, LOG_FILE_NAME)

    file_handler = RotatingFileHandler(
        file_path,
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT
    )
    file_handler.setFormatter(formatter)

    # -----------------------------
    # ADD HANDLERS
    # -----------------------------
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger