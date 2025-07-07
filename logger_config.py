import logging
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger

def setup_logging():
    """Configures and returns a logger that writes to a file."""
    logger = logging.getLogger("scim_connector")
    # Prevent duplicate handlers if this function is called multiple times
    if logger.handlers:
        return logger
        
    logger.setLevel(logging.INFO)
    
    log_formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
    
    # Create a handler that writes to 'scim_connector.log'
    log_handler = RotatingFileHandler('scim_connector.log', maxBytes=1048576, backupCount=5)
    log_handler.setFormatter(log_formatter)
    
    logger.addHandler(log_handler)
    
    return logger