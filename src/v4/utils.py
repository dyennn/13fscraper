"""
utils.py - Utility functions for 13F Scraper

Provides logging setup, timestamped logging, and ETA formatting utilities.
"""
import time
import logging
from typing import Optional

def setup_logger(log_file: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """
    Set up and return a logger for the application.
    Args:
        log_file (Optional[str]): If provided, log to this file as well as console.
        level (int): Logging level.
    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger("13f_scraper")
    logger.setLevel(level)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%H:%M:%S')
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        if log_file:
            fh = logging.FileHandler(log_file)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
    return logger

logger = setup_logger()

def log(msg: str, level: int = logging.INFO) -> None:
    """
    Log a message with a timestamp and level.
    Args:
        msg (str): Message to log.
        level (int): Logging level (e.g., logging.INFO, logging.ERROR).
    """
    logger.log(level, msg)

def fmt_eta(seconds: float) -> str:
    """
    Format seconds as HH:MM:SS for ETA display.
    Args:
        seconds (float): Number of seconds.
    Returns:
        str: Formatted time string.
    """
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"
