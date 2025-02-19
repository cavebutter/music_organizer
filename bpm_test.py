# This script is to be used on a different machine with local access to music library

import db.db_update as dbu
from loguru import logger
import sys
def configure_logging():
    logger.remove()
    logger.add(sys.stdout, level="DEBUG")
    logger.add("logs/bpm_test.log", rotation="10 MB", level="DEBUG")

if __name__ == '__main__':
    configure_logging()
    dbu.process_bpm(dbu.database, 'output/test_id_location.csv')