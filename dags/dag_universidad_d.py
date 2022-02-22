from datetime import timedelta, datetime
import logging
from time import strftime

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logging.getLogger('Universidad_d')