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
@Date   : 2021/5/18 15:11
@Author : 戴军 
@Email  : 18018030656@163.com
"""

import os
import platform
import traceback

SITE_ROOT = os.path.dirname(os.path.abspath(__file__))

OS = platform.system()
FORK_ID = None  # 进程序号

# 应用名称
APP_NAME = 'DJ'

# 服务器配置
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 8000
SERVER_PROTOCOL = 'https' if SERVER_PORT == 443 else 'http'

# 缓存设置
REDIS_CLUSTER = False
REDIS_NODES = [
    '127.0.0.1:6379'
]
REDIS_PASSWORD = None
REDIS_OPTIONS = dict(
    encoding='utf-8',
    decode_responses=False,
    max_connections=2 * 1024
)
REDIS_DB_INDEX = 8  # 库下标, REDIS_CLUSTER=True是该设置无效
REDIS_CACHED_TIMEOUT = 2 * 24 * 60 * 60  # 默认缓存超时时间(单位：秒)， 0：永不超时

DB_ADDRESS_LIST = [
    '127.0.0.1:27017',
]

DB_NAME = 'DJ'  # 数据名
DB_NAME_HIS = '%s_HIS' % DB_NAME
AUTH_USER_NAME = None  # 用户名
AUTH_USER_PASSWORD = None  # 密码
AUTH_DB_NAME = 'admin'  # 检验用户数据库
OPT_MIN_POOL_SIZE = 16  # 连接最小数量
OPT_MAX_POOL_SIZE = 512  # 连接最大数量
OPT_CONNECT_TIMEOUT_MS = 1000 * 3  # 连接超时时间, 单位: 毫秒
OPT_WAIT_QUEUE_TIMEOUT_MS = 1000 * 10  # 连接队列等待超时时间, 单位: 毫秒
OPT_REPLICA_SET_NAME = 'BMS_REP'  # 副本集名称
OPT_READ_PREFERENCE = 'secondaryPreferred'  # 副本集读写方式, primary|primaryPreferred|secondary|secondaryPreferred
OPT_WRITE_SYNC_NUMBER = 1  # 阻塞写操作直到同步指定数量的从服务器为止, 0: 禁用写确认, 使用事务是该值必须大于0，且小于等于从服务器数量
OPT_DISTRIBUTED_CACHED_ENABLE = True  # 启用数据库分布式缓存， 开启此选项请启用缓存
OPT_DISTRIBUTED_CACHED_TIMEOUT = REDIS_CACHED_TIMEOUT  # 数据库分布式缓存数据超时时间(单位：秒)， 0：永不超时

# 服务框架配置
DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
NUM_PROCESSES = 0  # 服务启动进程数，0：依据CPU数量启用进程数

# 系统日志
LOG_PATH = os.path.join(SITE_ROOT, 'logs')
LOG_LEVEL = 'ERROR'  # DEBUG|INFO|WARNING|ERROR|NONE
LOG_STDERR = False  # 输出到标准错误流
LOG_NAME = 'dj.log'  # 主日志
LOG_NAME_ACCESS = 'access.log'  # 执行日志
LOG_BACKUP_COUNT = 64  # 日志记录数量
