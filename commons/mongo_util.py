# !/usr/bin/python
# -*- coding:utf-8 -*-
"""
"""

import settings

from mongoengine import connect, disconnect, register_connection
from urllib.parse import quote_plus
from commons import logging

logger = logging.get_logging()


class MongoDBConf(object):
    _address_list = None
    _user_name = None
    _password = None
    _db = 'admin'
    _auth_db = 'admin'
    _min_pool_size = 16
    _max_pool_size = 128
    _connect_timeout_ms = 1000 * 3
    _wait_queue_timeout_ms = 1000 * 10
    _replica_set_name = None
    _read_preference = 'secondaryPreferred'
    _write_sync_number = 0
    _distributed_cached_enable = False

    _db_client = None

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(MongoDBConf, cls).__new__(cls, *args, **kwargs)
            cls._address_list = settings.DB_ADDRESS_LIST if settings.DB_ADDRESS_LIST else ['127.0.0.1:27017']
            cls._user_name = settings.AUTH_USER_NAME if settings.AUTH_USER_NAME else None
            cls._password = settings.AUTH_USER_PASSWORD if settings.AUTH_USER_PASSWORD else None
            cls._db = settings.DB_NAME if settings.DB_NAME else 'admin'
            cls._auth_db = settings.AUTH_DB_NAME if settings.AUTH_DB_NAME is not None else 'admin'
            cls._min_pool_size = int(settings.OPT_MIN_POOL_SIZE) if settings.OPT_MIN_POOL_SIZE is not None else 16
            cls._max_pool_size = int(settings.OPT_MAX_POOL_SIZE) if settings.OPT_MAX_POOL_SIZE is not None else 128
            cls._connect_timeout_ms = int(settings.OPT_CONNECT_TIMEOUT_MS) if settings.OPT_CONNECT_TIMEOUT_MS else 3000
            cls._wait_queue_timeout_ms = int(
                settings.OPT_WAIT_QUEUE_TIMEOUT_MS) if settings.OPT_WAIT_QUEUE_TIMEOUT_MS else 10000
            cls._replica_set_name = settings.OPT_REPLICA_SET_NAME
            cls._read_preference = settings.OPT_READ_PREFERENCE if settings.OPT_READ_PREFERENCE else 'secondaryPreferred'
            cls._write_sync_number = settings.OPT_WRITE_SYNC_NUMBER if settings.OPT_WRITE_SYNC_NUMBER else 0
            cls._distributed_cached_enable = True if settings.OPT_DISTRIBUTED_CACHED_ENABLE else False
        return cls._instance

    def _register(self):
        """
        注册数据库
        :return:
        """
        uri = self._get_connection_uri()
        logger.debug(f"connect addr:{uri}")
        self._db_client = connect(settings.DB_NAME, host=uri)
        if settings.DB_NAME_HIS:
            register_connection(settings.DB_NAME_HIS, settings.DB_NAME_HIS, host=uri)

    def client(self):
        if not self._db_client:
            self._register()
        return self._db_client

    def close_client(self):
        if self._db_client:
            disconnect(settings.DB_NAME)
            disconnect(settings.DB_NAME_HIS)

    def get_database(self, db_name=None):
        """
        获取数据库连接
        :param db_name: 数据库名称
        :return:
        """
        if not self._db_client:
            self._register()
        return self._db_client[db_name if db_name else self._db]

    def _get_connection_uri(self):
        """
        获取数据库连接URI
        :return: 连接选项(参考: https://docs.mongodb.com/manual/reference/connection-string/#connections-connection-options)
        """
        c_uri = None
        if self._address_list:
            if len(self._address_list) > 1 and not self._replica_set_name:
                raise ValueError('replicaSet must be include in parameter options .')

            address_comp = self.__get_address_component()
            options_comp = self.__get_options_component()
            if not self._auth_db and self._user_name and self._password:
                self._auth_db = 'admin'
            if self._auth_db:
                auth_comp = self.__get_auth_component()
                c_uri = 'mongodb://%s%s/%s%s' % (auth_comp, address_comp, self._auth_db, options_comp)
            else:
                c_uri = 'mongodb://%s/%s' % (address_comp, options_comp)
        return c_uri

    def __get_auth_component(self):
        """
        获取数据库校验位
        :param user_name: 用户名
        :param password: 密码
        :return: 字符串
        """
        if (self._user_name and not self._password) or (not self._user_name and self._password):
            raise ValueError('Both user_name and password must be empty or not empty at the same time.')
        if self._user_name and self._password:
            if not isinstance(self._user_name, str) or not isinstance(self._password, str):
                raise ValueError('Both user_name and password must be instance of str.')
            return '%s:%s@' % (quote_plus(self._user_name), quote_plus(self._password))
        return ''

    def __get_address_component(self):
        """
        获取数据库服务器地址位
        :return:
        """
        address_component = '127.0.0.1:27017'
        if self._address_list:
            address_component = ','.join(self._address_list)
        return address_component

    def __get_options_component(self):
        """
        获取数据库连接选项位
        :return:
        """
        options = dict(
            minPoolSize=self._min_pool_size,
            maxPoolSize=self._max_pool_size,
            connectTimeoutMS=self._connect_timeout_ms,
            waitQueueTimeoutMS=self._wait_queue_timeout_ms,
            connect='false',
        )
        if len(self._address_list) > 1 and self._replica_set_name:
            options['replicaSet'] = self._replica_set_name
            options['readPreference'] = self._read_preference
            options['w'] = self._write_sync_number

        options_component = ''
        if options:
            for key, val in options.items():
                if not key == 'cached_option':
                    options_component += "%s=%s&" % (key, val)
            options_component = '?' + options_component[:-1]
        return options_component
