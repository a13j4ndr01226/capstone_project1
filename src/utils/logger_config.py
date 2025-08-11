"""
Configures a reusable logger for pipeline scripts.

This module sets up a logger named "pipeline_logger" that logs messages to both the console
and a log file ("logs/pipeline.log") in append mode. The log format includes timestamp, 
log level, and message. The logger is intended for use across multiple scripts in a pipeline
to provide consistent logging behavior.

Typical usage example:
    from utils.logger_config import logger

    logger.info("Starting ETL job...")
    logger.error("Failed to connect to database.")

Attributes:
    logger (logging.Logger): Configured logger instance for pipeline-wide use.
"""

import os
import logging


#Ensure Logs directory exists
os.makedirs("logs", exist_ok=True)

#Create Logger
logger = logging.getLogger("pipeline_logger") #use __name__ for larger projects where different verbosity for different scripts are needed
logger.setLevel(logging.INFO)

#Create Log format
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

#Create File handler
file_handler = logging.FileHandler("logs/pipeline.log")
file_handler.setFormatter(formatter)

#Create Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

#Add handlers to Logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Avoid adding handlers multiple times if already configured
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

__all__ = ["logger"]