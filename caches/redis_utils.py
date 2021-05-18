# !/usr/bin/python

import datetime
import asyncio
import functools

from redis import StrictRedis, ConnectionPool
from aioredis import (
    create_pool as async_create_pool,
    Redis as AioRedis
)
from aioredis.commands.transaction import Pipeline, MultiExec

import settings
from caches.LuaManager import LuaDict


class _AioContext(object):
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __getattr__(self, name):
        assert not self._done, "Pipeline already executed. Create new one."
        attr = getattr(self._redis, name)
        if callable(attr):

            @functools.wraps(attr)
            async def wrapper(*args, **kw):
                try:
                    task = asyncio.ensure_future(attr(*args, **kw))
                except Exception as exc:
                    task = asyncio.get_event_loop().create_future()
                    task.set_exception(exc)
                self._results.append(task)
            return wrapper
        return attr


class _AioPipeline(_AioContext, Pipeline):
    pass


class _AioMultiExec(_AioContext, MultiExec):
    def __init__(self, client, watches=None):
        super(_AioMultiExec, self).__init__(client._pool_or_conn, client.__class__)
        self._watches = watches
        self._client = client

    async def __aenter__(self):
        if self._watches:
            await self._client.watch(*self._watches)
        return self

    async def __aexit__(self, *args):
        if self._watches:
            await self._client.unwatch()

    def multi(self):
        pass


class AIORedisDB(object):
    def __init__(self):
        self.__redis = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(AIORedisDB, cls).__new__(cls, *args, **kwargs)
            cls._startup_nodes = settings.REDIS_NODES
            cls._db_index = settings.REDIS_DB_INDEX
        return cls._instance

    async def setup(self):
        if not self.__redis:
            class _AioRedis(AioRedis):
                def set(self, key, value, ex=None, px=None, nx=None, xx=None):
                    exist = None
                    if nx:
                        exist = self.SET_IF_NOT_EXIST
                    elif xx:
                        exist = self.SET_IF_EXIST
                    return super(_AioRedis, self).set(key, value,
                                                      expire=ex or 0,
                                                      pexpire=px or 0,
                                                      exist=exist)

                def incr(self, key, increment=1):
                    return super(_AioRedis, self).incrby(key, increment)

                def pipeline(self, is_transaction=False, watches=None, shard_hint=None):
                    if is_transaction:
                        return _AioMultiExec(self, watches=watches)
                    else:
                        return _AioPipeline(self._pool_or_conn, self.__class__)

                def hmset(self, key, kv_dict):
                    pairs = []
                    for k, v in kv_dict.items():
                        pairs.append(k)
                        pairs.append(v)
                    return super(_AioRedis, self).hmset(key, *pairs)

                def hmget(self, key, fields):
                    return super(_AioRedis, self).hmget(key, *fields)

                def zrangebyscore(self, name, min, max, start=None, num=None, withscores=False):
                    return super(_AioRedis, self).zrangebyscore(
                        name, min, max, withscores=withscores, offset=start, count=num
                    )

                def zrange(self, name, start, end, desc=False, withscores=False):
                    if desc:
                        return self.zrevrange(name, start, end, withscores)
                    return super(_AioRedis, self).zrange(name, start, end, withscores)

            if hasattr(settings, 'REDIS_MOCK'):
                if settings.REDIS_MOCK:
                    from .redis_fake import AsyncFakeStrictRedis
                    self.__redis = AsyncFakeStrictRedis(
                        db=self._db_index, **settings.REDIS_OPTIONS)
                    return self.__redis

            default_node = self._startup_nodes[0]
            if not default_node:
                raise ValueError('Redis server node not specified.')
            host, port = default_node.split(':')
            if not host:
                raise ValueError('Redis server host not specified.')
            if not port:
                port = 6379
            password = settings.REDIS_PASSWORD
            connection_pool = await async_create_pool(
                (host, port), db=self._db_index, password=password)
            self.__redis = _AioRedis(connection_pool)

        return self.__redis

    @property
    def __rc(self):
        return self.__redis

    @property
    def db(self):
        return self.__rc

    @property
    def pipeline(self):
        """
        开启处理管道
        :param transaction:
        :param shard_hint:
        :return:
        """
        return self.__rc.pipeline()

    def transaction(self, watches=None, shard_hint=None):
        """
        开启事务
        :param watches: 需要watch的key列表
        :param shard_hint:
        """
        return self.__rc.pipeline(True, watches=watches, shard_hint=shard_hint)

    async def set(self, name, value=None, timeout=settings.REDIS_CACHED_TIMEOUT, existed=None):
        """
        设置值
        :param name: 键
        :param value: 值
        :param timeout: 超时时间，None 时永不超时
        :param existed: True 已存在时设置， False 不存在时设置， None 所有情况下设置
        :return:
        """
        if existed is None:
            await self.__rc.set(name, value, ex=timeout)
        elif existed is False:
            await self.__rc.set(name, value, ex=timeout, nx=True)
        elif existed is True:
            await self.__rc.set(name, value, ex=timeout, xx=True)

    async def setnx(self, name, value=None, timeout=settings.REDIS_CACHED_TIMEOUT):
        """
        检查name是否已设置，如果没有设置则设置为value
        :param name: 键
        :param value: 值
        :param timeout: 超时时间，None 时永不超时
        :return: True: 设置成功
                 False: 已设置
        """
        if value is None:
            value = ''
        if 0 == await self.__rc.set(name, value, ex=timeout, nx=True):
            return False
        return True

    async def get(self, name):
        """
        获取值
        :param name: 键
        :return: 从Redis获取到的值
        """
        return await self.__rc.get(name)

    async def exists(self, name):
        """
        判断名为name的键是否存在
        :param name:
        :return:
        """
        return await self.__rc.exists(name)

    async def delete(self, name):
        """
        删除值
        :param name: 键
        :return:
        """
        await self.__rc.delete(name)

    async def incr(self, name, amount=1):
        """
        自增
        :param name: 键
        :param amount: 步长
        :return:
        """
        return await self.__rc.incr(name, amount)

    async def mset(self, **kwargs):
        """
        批量设值
        :param kwargs: 参数列表（key-value）
        :return:
        """
        await self.__rc.mset(**kwargs)

    async def mget(self, names):
        """
        批量获取值
        :param keys: 键列表，List或Tuple
        :return: 返回对应值， List或Tuple，与参数形式对应
        """
        return await self.__rc.mget(names)

    async def hset(self, name, key, value=None):
        """
        设置一个dict形式的值
        :param name: 键
        :param key: 值键
        :param value: 值
        :return:
        """
        await self.__rc.hset(name, key, value)

    async def hget(self, name, key):
        """
        获取一个dict形式的值
        :param name: 键
        :param key: 值键
        :return:
        """
        return await self.__rc.hget(name, key)

    async def hmset(self, name, kv_dict=None):
        """
        批量设置dict形式的值
        :param name: 键
        :param kv_dict: 值(dict)
        :return:
        """
        await self.__rc.hmset(name, kv_dict)

    async def hmget(self, name, keys):
        """
        批量获取dict形式的值
        :param name: 键
        :param keys: 值键List或Tuple，与参数形式对应
        :return:
        """
        return await self.__rc.hmget(name, keys)

    async def hgetall(self, name):
        """
        获取dict形式所有的值
        :param name:
        :return:
        """
        return await self.__rc.hgetall(name)

    async def hlen(self, name):
        """
        获取dict形式值的个数
        :param name: 键
        :return:
        """
        return await self.__rc.hlen(name)

    async def hkeys(self, name):
        """
        获取dict形式所有的键
        :param name: 键
        :return:
        """
        return await self.__rc.hkeys(name)

    async def hvals(self, name):
        """
        获取dict形式所有的值
        :param name: 键
        :return:
        """
        return await self.__rc.hvals(name)

    async def hexists(self, name, key):
        """
        判断值键是否存在
        :param name:
        :param key:
        :return:
        """
        return await self.__rc.hexists(name, key)

    async def hdel(self, name, *keys):
        """
        获取dict形式多个key
        :param name: 键
        :param keys: 值键Tuple
        :return:
        """
        await self.__rc.hdel(name, *keys)

    async def lrange(self, name, start, end):
        """
        获取list指定范围的集合
        :param name:
        :param start:
        :param end:
        :return:
        """
        return await self.__rc.lrange(name, start, end)

    async def lpush(self, name, *vals):
        """
        元素追加到列表左
        :param name: 键
        :param vals: 多个值
        :return:
        """
        await self.__rc.lpush(name, *vals)

    async def rpush(self, name, *values):
        """
        元素追加到列表右
        :param name:
        :param values:
        :return:
        """
        await self.__rc.rpush(name, *values)

    async def rpop(self, name):
        """
        移除并返回列表 name 的尾元素
        :param name:
        :return: 列表的尾元素。 当 name 不存在时，返回 nil
        """
        return await self.__rc.rpop(name)

    async def lget(self, name, index):
        """
        取指定位置的值
        :param name: 键
        :param index: 索引值
        :return:
        """
        return await self.__rc.lindex(name, index)

    async def ldel(self, name, value):
        """
        删除值
        :param name: 键
        :param value: 值
        :return:
        """
        await self.__rc.lrem(name, 1, value)

    async def sadd(self, name, vals):
        """
        想集合中添加元素
        :param name: 键
        :param vals: 多个值
        :return:
        """
        await self.__rc.sadd(name, vals)

    async def slen(self, name):
        """
        获取集合元素个数
        :param name: 键
        :return: 集合长度
        """
        return await self.__rc.scard(name)

    async def smembers(self, name):
        """
        获取集合所有成员
        :param name: 键
        :return: 所有成员
        """
        return await self.__rc.smembers(name)

    async def sismember(self, name, value):
        """
        判断值是否存在与集合中
        :param name: 键
        :param value: 值
        :return:
        """
        return await self.__rc.sismember(name, value)

    async def sdel(self, name):
        """
        从集合中删除成员
        :param name:
        :return:
        """
        await self.__rc.spop(name)

    async def set_expire(self, name, seconds: int = settings.REDIS_CACHED_TIMEOUT):
        """
        设置健超时时长
        :param name:
        :param seconds:
        :return:
        """
        if name and seconds:
            return await self.__rc.expire(name, seconds)

    async def set_expire_dt(self, name, dt: datetime.datetime):
        """
        设置健超时时间
        :param name:
        :param dt:
        :return:
        """
        if name and dt and isinstance(dt, datetime.datetime):
            if dt > datetime.datetime.now():
                return await self.__rc.expireat(name, dt)

    async def register_script(self, script):
        return await self.db.register_script(script)

    async def evalsha(self, sha, numkeys, *keys_and_args):
        return await self.db.evalsha(sha, numkeys, *keys_and_args)

    async def run_script(self, script_name, keys, args):
        """
        通用的脚本运行器
        :param script_name: 脚本名称
        :param keys: 可迭代对象（key1, key2...）
        :param args: 可迭代对象（key1, key2...）
        :return:
        """
        if script_name in self.LuaDict:
            return await self.LuaDict[script_name].run_script(keys, args)
        else:
            raise Exception(u"暂时未定义该脚本")

    async def _flushall(self, *args):
        await self.__rc.flushall(*args)


class RedisDB(object):
    __redis_cluster = None

    def __init__(self):
        self.LuaDict = {key: redis_lua(self.db)
                        for key, redis_lua in LuaDict.items()}

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(RedisDB, cls).__new__(cls, *args, **kwargs)
            cls._cluster = settings.REDIS_CLUSTER
            cls._startup_nodes = settings.REDIS_NODES
            cls._db_index = 0 if cls._cluster else settings.REDIS_DB_INDEX
            if cls._cluster:
                if not cls._startup_nodes:
                    raise ValueError('Redis cluster nodes not specified.')
        return cls._instance

    @property
    def __rc(self):
        if not self.__redis_cluster:
            if hasattr(settings, 'REDIS_MOCK'):
                if settings.REDIS_MOCK:
                    from fakeredis import FakeStrictRedis
                    self.__redis_cluster = FakeStrictRedis()
                    return self.__redis_cluster

            if self._cluster:
                from rediscluster import RedisCluster
                startup_nodes = []
                for node in self._startup_nodes:
                    if node:
                        host, port = node.split(':')
                        if not host or not port:
                            raise ValueError('Redis cluster nodes host error.')
                        startup_nodes.append({'host': host, 'port': port})
                if not startup_nodes:
                    raise ValueError('Redis cluster nodes not specified.')
                password = settings.REDIS_PASSWORD
                self.__redis_cluster = RedisCluster(
                    startup_nodes=startup_nodes, password=password, **settings.REDIS_OPTIONS)

            else:
                default_node = self._startup_nodes[0]
                if not default_node:
                    raise ValueError('Redis server node not specified.')
                host, port = default_node.split(':')
                if not host:
                    raise ValueError('Redis server host not specified.')
                if not port:
                    port = 6379
                password = settings.REDIS_PASSWORD
                connection_pool = ConnectionPool(
                    host=host, port=port, password=password, db=self._db_index)
                self.__redis_cluster = StrictRedis(
                    connection_pool=connection_pool, **settings.REDIS_OPTIONS)

        return self.__redis_cluster

    @property
    def db(self):
        return self.__rc

    @property
    def pipeline(self):
        """
        开启处理管道
        :param transaction:
        :param shard_hint:
        :return:
        """
        return self.__rc.pipeline()

    def transaction(self, watches=None, shard_hint=None):
        """
        开启事务
        :param watches: 需要watch的key列表
        :param shard_hint:
        """
        return self.__rc.pipeline(True, watches=watches, shard_hint=shard_hint)

    def set(self, name, value=None, timeout=settings.REDIS_CACHED_TIMEOUT, existed=None):
        """
        设置值
        :param name: 键
        :param value: 值
        :param timeout: 超时时间，None 时永不超时
        :param existed: True 已存在时设置， False 不存在时设置， None 所有情况下设置
        :return:
        """
        if existed is None:
            self.__rc.set(name, value, ex=timeout)
        elif existed is False:
            self.__rc.set(name, value, ex=timeout, nx=True)
        elif existed is True:
            self.__rc.set(name, value, ex=timeout, xx=True)

    def setnx(self, name, value=None, timeout=settings.REDIS_CACHED_TIMEOUT):
        """
        检查name是否已设置，如果没有设置则设置为value
        :param name: 键
        :param value: 值
        :param timeout: 超时时间，None 时永不超时
        :return: True: 设置成功
                 False: 已设置
        """
        if value is None:
            value = ''
        if 0 == self.__rc.set(name, value, ex=timeout, nx=True):
            return False
        return True

    def get(self, name):
        """
        获取值
        :param name: 键
        :return: 从Redis获取到的值
        """
        return self.__rc.get(name)

    def exists(self, name):
        """
        判断名为name的键是否存在
        :param name:
        :return:
        """
        return self.__rc.exists(name)

    def delete(self, name):
        """
        删除值
        :param name: 键
        :return:
        """
        self.__rc.delete(name)

    def incr(self, name, amount=1):
        """
        自增
        :param name: 键
        :param amount: 步长
        :return:
        """
        return self.__rc.incr(name, amount)

    def mset(self, **kwargs):
        """
        批量设值
        :param kwargs: 参数列表（key-value）
        :return:
        """
        self.__rc.mset(**kwargs)

    def mget(self, names):
        """
        批量获取值
        :param keys: 键列表，List或Tuple
        :return: 返回对应值， List或Tuple，与参数形式对应
        """
        return self.__rc.mget(names)

    def hset(self, name, key, value=None):
        """
        设置一个dict形式的值
        :param name: 键
        :param key: 值键
        :param value: 值
        :return:
        """
        self.__rc.hset(name, key, value)

    def hget(self, name, key):
        """
        获取一个dict形式的值
        :param name: 键
        :param key: 值键
        :return:
        """
        return self.__rc.hget(name, key)

    def hmset(self, name, kv_dict=None):
        """
        批量设置dict形式的值
        :param name: 键
        :param kv_dict: 值(dict)
        :return:
        """
        self.__rc.hmset(name, kv_dict)

    def hmget(self, name, keys):
        """
        批量获取dict形式的值
        :param name: 键
        :param keys: 值键List或Tuple，与参数形式对应
        :return:
        """
        return self.__rc.hmget(name, keys)

    def hgetall(self, name):
        """
        获取dict形式所有的值
        :param name:
        :return:
        """
        return self.__rc.hgetall(name)

    def hlen(self, name):
        """
        获取dict形式值的个数
        :param name: 键
        :return:
        """
        return self.__rc.hlen(name)

    def hkeys(self, name):
        """
        获取dict形式所有的键
        :param name: 键
        :return:
        """
        return self.__rc.hkeys(name)

    def hvals(self, name):
        """
        获取dict形式所有的值
        :param name: 键
        :return:
        """
        return self.__rc.hvals(name)

    def hexists(self, name, key):
        """
        判断值键是否存在
        :param name:
        :param key:
        :return:
        """
        return self.__rc.hexists(name, key)

    def hdel(self, name, *keys):
        """
        获取dict形式多个key
        :param name: 键
        :param keys: 值键Tuple
        :return:
        """
        self.__rc.hdel(name, *keys)

    def lrange(self, name, start, end):
        """
        获取list指定范围的集合
        :param name:
        :param start:
        :param end:
        :return:
        """
        return self.__rc.lrange(name, start, end)

    def lpush(self, name, *vals):
        """
        元素追加到列表左
        :param name: 键
        :param vals: 多个值
        :return:
        """
        self.__rc.lpush(name, *vals)

    def rpush(self, name, *values):
        """
        元素追加到列表右
        :param name:
        :param values:
        :return:
        """
        self.__rc.rpush(name, *values)

    def rpop(self, name):
        """
        移除并返回列表 name 的尾元素
        :param name:
        :return: 列表的尾元素。 当 name 不存在时，返回 nil
        """
        return self.__rc.rpop(name)

    def lget(self, name, index):
        """
        取指定位置的值
        :param name: 键
        :param index: 索引值
        :return:
        """
        return self.__rc.lindex(name, index)

    def ldel(self, name, value):
        """
        删除值
        :param name: 键
        :param value: 值
        :return:
        """
        self.__rc.lrem(name, 1, value)

    def sadd(self, name, vals):
        """
        想集合中添加元素
        :param name: 键
        :param vals: 多个值
        :return:
        """
        self.__rc.sadd(name, vals)

    def slen(self, name):
        """
        获取集合元素个数
        :param name: 键
        :return: 集合长度
        """
        return self.__rc.scard(name)

    def smembers(self, name):
        """
        获取集合所有成员
        :param name: 键
        :return: 所有成员
        """
        return self.__rc.smembers(name)

    def sismember(self, name, value):
        """
        判断值是否存在与集合中
        :param name: 键
        :param value: 值
        :return:
        """
        return self.__rc.sismember(name, value)

    def sdel(self, name):
        """
        从集合中删除成员
        :param name:
        :return:
        """
        self.__rc.spop(name)

    def set_expire(self, name, seconds: int = settings.REDIS_CACHED_TIMEOUT):
        """
        设置健超时时长
        :param name:
        :param seconds:
        :return:
        """
        if name and seconds:
            return self.__rc.expire(name, seconds)

    def set_expire_dt(self, name, dt: datetime.datetime):
        """
        设置健超时时间
        :param name:
        :param dt:
        :return:
        """
        if name and dt and isinstance(dt, datetime.datetime):
            if dt > datetime.datetime.now():
                return self.__rc.expireat(name, dt)

    def register_script(self, script):
        return self.db.register_script(script)

    def evalsha(self, sha, numkeys, *keys_and_args):
        return self.db.evalsha(sha, numkeys, *keys_and_args)

    def run_script(self, script_name, keys, args):
        """
        通用的脚本运行器
        :param script_name: 脚本名称
        :param keys: 可迭代对象（key1, key2...）
        :param args: 可迭代对象（key1, key2...）
        :return:
        """
        if script_name in self.LuaDict:
            return self.LuaDict[script_name].run_script(keys, args)
        else:
            raise Exception(u"暂时未定义该脚本")

    def _flushall(self, *args):
        self.__rc.flushall(*args)


RedisCache = RedisDB()
AsyncRedisCache = AIORedisDB()


if __name__ == '__main__':
    import time
    async def test1():
        cache = AIORedisDB()
        await cache.setup()
        await asyncio.wait([cache.get('x') for _ in range(10000)])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(test1())
