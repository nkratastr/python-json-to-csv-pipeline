"""
Logger Configuration Module
Sets up logging for the entire pipeline with file rotation and proper formatting.
"""

import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional
import yaml


def setup_logger(
    name: str,
    log_dir: str = "logs",
    log_file: str = "pipeline.log",
    level: str = "INFO",
    log_format: Optional[str] = None,
    max_bytes: int = 10485760,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Set up a logger with file rotation and console output.
    
    Args:
        name: Name of the logger
        log_dir: Directory to store log files
        log_file: Name of the log file
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        max_bytes: Maximum size of log file before rotation (default: 10MB)
        backup_count: Number of backup log files to keep
    
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Prevent duplicate handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Define format
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    
    formatter = logging.Formatter(log_format)
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_path / log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
    
    Returns:
        Configuration dictionary
    
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing configuration file: {e}")


def get_logger_from_config(config_path: str = "config/config.yaml") -> logging.Logger:
    """
    Create a logger using settings from the configuration file.
    
    Args:
        config_path: Path to the configuration file
    
    Returns:
        Configured logger instance
    """
    try:
        config = load_config(config_path)
        
        logging_config = config.get('logging', {})
        paths_config = config.get('paths', {})
        
        logger = setup_logger(
            name=config.get('pipeline', {}).get('name', 'json_to_csv_pipeline'),
            log_dir=paths_config.get('log_dir', 'logs'),
            log_file=logging_config.get('file_name', 'pipeline.log'),
            level=logging_config.get('level', 'INFO'),
            log_format=logging_config.get('format'),
            max_bytes=logging_config.get('max_bytes', 10485760),
            backup_count=logging_config.get('backup_count', 5)
        )
        
        logger.info(f"Logger initialized from config: {config_path}")
        return logger
        
    except Exception as e:
        # Fallback to basic logger if config loading fails
        logger = setup_logger(name='json_to_csv_pipeline')
        logger.warning(f"Failed to load config, using default settings: {e}")
        return logger


# Convenience function to get a logger quickly
def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance. If name is None, returns the root pipeline logger.
    
    Args:
        name: Logger name (optional)
    
    Returns:
        Logger instance
    """
    if name is None:
        return logging.getLogger('json_to_csv_pipeline')
    return logging.getLogger(name)
