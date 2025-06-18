import logging
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os

class Logger:
    """
    A flexible logging class that supports console and file logging with different log levels.
    
    Features:
    - Log to console and/or file
    - Rotating file handler to prevent large log files
    - Customizable log format
    - Multiple log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - Timestamp in log messages
    """
    
    def __init__(self, name, log_level=logging.INFO, log_to_console=True, log_to_file=False, 
                 log_file='app.log', max_file_size=5*1024*1024, backup_count=5):
        """
        Initialize the logger.
        
        Args:
            name (str): Name of the logger
            log_level (int): Logging level (e.g., logging.DEBUG, logging.INFO)
            log_to_console (bool): Whether to log to console
            log_to_file (bool): Whether to log to file
            log_file (str): Path to log file
            max_file_size (int): Max file size in bytes before rotation (default: 5MB)
            backup_count (int): Number of backup files to keep
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        
        # Clear any existing handlers to avoid duplicate logs
        self.logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        if log_to_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # File handler with rotation
        if log_to_file:
            # Create log directory if it doesn't exist
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
                
            file_handler = RotatingFileHandler(
                log_file, 
                maxBytes=max_file_size, 
                backupCount=backup_count
            )
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def debug(self, message):
        """Log a debug message."""
        self.logger.debug(message)
    
    def info(self, message):
        """Log an info message."""
        self.logger.info(message)
    
    def warning(self, message):
        """Log a warning message."""
        self.logger.warning(message)
    
    def error(self, message):
        """Log an error message."""
        self.logger.error(message)
    
    def critical(self, message):
        """Log a critical message."""
        self.logger.critical(message)
    
    def exception(self, message):
        """Log an exception message with stack trace."""
        self.logger.exception(message)
    
    def log(self, level, message):
        """Log a message at the specified level."""
        self.logger.log(level, message)
    
    def set_level(self, level):
        """Set the logging level for all handlers."""
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)


# Example usage
if __name__ == "__main__":
    # Create logger that logs to both console and file
    logger = Logger(
        name="MyApp",
        log_level=logging.DEBUG,
        log_to_console=True,
        log_to_file=True,
        log_file="logs/myapp.log"
    )
    
    # Log messages at different levels
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("An exception occurred:")