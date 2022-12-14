import logging
from datetime import datetime
import os

# 로그
logger = logging.getLogger()
# log level : INFO
logger.setLevel(logging.INFO)
# log format : [INFO][yyyy/MM/dd HH:mm:ss] message
formatter = logging.Formatter('[%(levelname)s][%(asctime)s][%(process)s] %(message)s')
# log stream 출력 설정 : StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# log file : FileHandler
date = datetime.now().strftime('%Y-%m-%d')
directory = '../tmp/capitol-trades-log'
os.makedirs(directory, exist_ok=True)
file_handler = logging.FileHandler(directory + '/capital-trades_{date}.log'.format(date=date))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
