import logging


# Basic configuration of the format and instantiation of the logger
logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d')
logger = logging.getLogger('univ_g')