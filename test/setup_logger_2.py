import logging_config
import logging

from logging_config import module_logger

module_logger_str = f"({module_logger.name}: {module_logger.level})"

newlogger = logging.getLogger(__name__)
newlogger_str = f"({newlogger.name}: {newlogger.level})"


if __name__ == "__main__": 
 
    print(module_logger_str) # now has the name from the module it's in 
    print(newlogger_str)