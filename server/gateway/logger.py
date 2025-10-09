"""
Centralized logging configuration for the Coffee Shop Analysis Gateway
"""
import logging
import sys
from typing import Optional


class GatewayLogger:
    """Singleton logger class for the gateway service"""
    
    _instance: Optional['GatewayLogger'] = None
    _initialized: bool = False
    
    def __new__(cls) -> 'GatewayLogger':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._setup_logging()
            self._initialized = True
    
    def _setup_logging(self):
        """Configure the logging format and handlers"""
        # Create formatter with line numbers
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # Clear any existing handlers to avoid duplicates
        root_logger.handlers.clear()
        root_logger.addHandler(console_handler)
        
        # Prevent propagation to avoid duplicate messages
        root_logger.propagate = False
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger instance for the specified module name
        
        Args:
            name: Usually __name__ from the calling module
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        return logger
    
    def set_level(self, level: int):
        """
        Set the logging level for all loggers
        
        Args:
            level: Logging level (e.g., logging.DEBUG, logging.INFO, etc.)
        """
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        for handler in root_logger.handlers:
            handler.setLevel(level)


# Convenience function to get a logger
def get_logger(name: str) -> logging.Logger:
    """
    Convenience function to get a configured logger
    
    Usage:
        from gateway.logger import get_logger
        logger = get_logger(__name__)
    
    Args:
        name: Usually __name__ from the calling module
        
    Returns:
        Configured logger instance with line numbers
    """
    gateway_logger = GatewayLogger()
    return gateway_logger.get_logger(name)


# Initialize logging when module is imported
_gateway_logger = GatewayLogger()