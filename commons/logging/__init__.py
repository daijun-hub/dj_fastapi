# coding:utf-8
"""                          _
_._ _..._ .-',     _.._(`))
'-. `     '  /-._.-'    ',/
    )         \            '.
   / _    _    |             \
  |  a    a    /              |
  \   .-.                     ;
   '-('' ).-'       ,'       ;
      '-;           |      .'
         \           \    /
         | 7  .__  _.-\   \
         | |  |  ``/  /`  /
        /,_|  |   /,_/   /
           /,_/      '`-'
@Date   : 2021/5/18 15:16
@Author : 戴军 
@Email  : 18018030656@163.com
"""
import logging
import os
import sys

import settings
from commons.logging.logger import ConcurrentTimedRotatingFileHandler

DEFAULT_DATE_FORMAT = '%y%m%d %H:%M:%S'
DEFAULT_FORMAT = '[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s'

LOGGING_MAPPING = {}


def get_logging(name=None, file_name=None) -> logging.Logger:
    global LOGGING_MAPPING
    name = str(name) if name else 'default'
    file_name = str(file_name) if file_name else settings.LOG_NAME
    log_key = ('%s_%s' % (name, file_name)).replace('.', '_')
    if log_key not in LOGGING_MAPPING.keys():
        _generate_logger(name, file_name)
    return LOGGING_MAPPING.get(log_key)


def _generate_logger(name=None, file_name=None):
    global LOGGING_MAPPING
    # 依据名称生成日志对象
    log = logging.getLogger(name if name else 'default')
    log_file = os.path.join(
        settings.LOG_PATH, file_name) if file_name else os.path.join(settings.LOG_PATH, settings.LOG_NAME)
    channel_handler = ConcurrentTimedRotatingFileHandler(
        filename=log_file, when='MIDNIGHT', interval=1, backup_count=settings.LOG_BACKUP_COUNT)
    channel_handler.setFormatter(logging.Formatter(DEFAULT_FORMAT, datefmt=DEFAULT_DATE_FORMAT))
    log.addHandler(channel_handler)
    if settings.LOG_STDERR and name != 'tornado.access':
        console_channel = logging.StreamHandler(sys.stderr)
        console_channel.setFormatter(logging.Formatter(DEFAULT_FORMAT))
        log.addHandler(console_channel)
    # 设置日志等级
    log.setLevel(settings.LOG_LEVEL)
    log_key = ('%s_%s' % (name, file_name)).replace('.', '_')
    LOGGING_MAPPING[log_key] = log

