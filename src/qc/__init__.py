import logging

# customize Exception
class MissingError(Exception):
    pass

# customize logger
logger = logging.getLogger(__name__)