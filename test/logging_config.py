from ..config import get_logger, set_logging_level
import logging 

logger = get_logger()
set_logging_level(logging.DEBUG)

if not logger.hasHandlers(): 
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
