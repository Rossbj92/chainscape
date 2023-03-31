import logging
import time
from datetime import datetime
import os

LOGS_DIR = 'logs/chainscape'

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

logger = logging.getLogger("chainscape")

prog_time = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)

formatter = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(f"{LOGS_DIR}/chainscape - {prog_time}.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

