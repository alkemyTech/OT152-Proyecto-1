import logging
from datetime import timedelta
from time import strftime

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidad_d')

#configuro los retries acorde a lo que pide la tarea
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
