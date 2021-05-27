import datetime
import logging
import logging.handlers as handlers
import os
import coloredlogs
from os import path, mkdir


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)-10s %(name)-12s %(levelname)-8s %(message)s')

def convert_to_utc_8(sec, what):
    utc8_time = datetime.datetime.now() + datetime.timedelta(hours=8)
    return utc8_time.timetuple()

def create_logger(message, log_dir='./'):
    if not path.exists(f'{log_dir}log'):
        mkdir(f'{log_dir}log')

    logging.Formatter.converter = convert_to_utc_8
    coloredlogs.install()
    logger = logging.getLogger(message)
    logHandler = handlers.RotatingFileHandler(f'{log_dir}log/{message}.log', 'a', maxBytes=50*1024*1024, backupCount=5)
    logHandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(threadName)-10s %(name)-12s %(levelname)-4s %(message)s')
    logHandler.setFormatter(formatter)
    
    logger.addHandler(logHandler)
    
    return logger

def log_to_file(filePath : str, message : str):
    """ Write a single line log message to a target file
    
    Arguments:
        filePath {str} -- Path of log file
        message {str} -- Message to be written
    """

    if os.path.exists(filePath):
        openType = 'a'
    else:
        openType = 'w'

    with open(filePath, openType) as file:
        file.write(f'{message}\n')