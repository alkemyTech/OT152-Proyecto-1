from datetime import timedelta
import logging


# Basic configuration of the format and instantiation of the logger
logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d')
logger = logging.getLogger('univ_g')

default_args = {
    "retries": 5,  # Try 5 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}