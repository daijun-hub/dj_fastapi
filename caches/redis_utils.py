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
        ??????????????????
        :param transaction:
        :param shard_hint:
        :return:
        """
        return self.__rc.pipeline()

    def transaction(self, watches=None, shard_hint=None):
        """
        ????????????
        :param watches: ??????watch???key??????
        :param shard_hint:
        """
        return self.__rc.pipeline(True, watches=watches, shard_hint=shard_hint)

    async def set(self, name, value=None, timeout=settings.REDIS_CACHED_TIMEOUT, existed=None):
        """
        ?????????
        :param name: ???
        :param value: ???
        :param timeout: ???????????????None ???????????????
        :param existed: True ????????????????????? False ????????????????????? None ?????????????????????
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
        ??????name????????????????????????????????????????????????value
        :param name: ???
        :param value: ???
        :param timeout: ???????????????None ???????????????
        :return: True: ????????????
                 False: ?????????
        """
        if value is None:
            value = ''
        if 0 == await self.__rc.set(name, value, ex=timeout, nx=True):
            return False
        return True

    async def get(self, name):
        """
        ?????????
        :param name: ???
        :return: ???Redis???????????????
        """
        return await self.__rc.get(name)

    async def exists(self, name):
        """
        ????????????name??????????????????
        :param name:
        :return:
        """
        return await self.__rc.exists(name)

    async def delete(self, name):
        """
        ?????????
        :param name: ???
        :return:
        """
        await self.__rc.delete(name)

    async def incr(self, name, amount=1):
        """
        ??????
        :param name: ???
        :param amount: ??????
        :return:
        """
        return await self.__rc.incr(name, amount)

    async def mset(self, **kwargs):
        """
        ????????????
        :param kwargs: ???????????????key-value???
        :return:
        """
        await self.__rc.mset(**kwargs)

    async def mget(self, names):
        """
        ???????????????
        :param keys: ????????????List???Tuple
        :return: ?????????????????? List???Tuple????????????????????????
        """
        return await self.__rc.mget(names)

    async def hset(self, name, key, value=None):
        """
        ????????????dict????????????
        :param name: ???
        :param key: ??????
        :param value: ???
        :return:
        """
        await self.__rc.hset(name, key, value)

    async def hget(self, name, key):
        """
        ????????????dict????????????
        :param name: ???
        :param key: ??????
        :return:
        """
        return await self.__rc.hget(name, key)

    async def hmset(self, name, kv_dict=None):
        """
        ????????????dict????????????
        :param name: ???
        :param kv_dict: ???(dict)
        :return:
        """
        await self.__rc.hmset(name, kv_dict)

    async def hmget(self, name, keys):
        """
        ????????????dict????????????
        :param name: ???
        :param keys: ??????List???Tuple????????????????????????
        :return:
        """
        return await self.__rc.hmget(name, keys)

    async def hgetall(self, name):
        """
        ??????dict??????????????????
        :param name:
        :return:
        """
        return await self.__rc.hgetall(name)

    async def hlen(self, name):
        """
        ??????dict??????????????????
        :param name: ???
        :return:
        """
        return await self.__rc.hlen(name)

    async def hkeys(self, name):
        """
        ??????dict??????????????????
        :param name: ???
        :return:
        """
        return await self.__rc.hkeys(name)

    async def hvals(self, name):
        """
        ??????dict??????????????????
        :param name: ???
        :return:
        """
        return await self.__rc.hvals(name)

    async def hexists(self, name, key):
        """
        ????????????????????????
        :param name:
        :param key:
        :return:
        """
        return await self.__rc.hexists(name, key)

    async def hdel(self, name, *keys):
        """
        ??????dict????????????key
        :param name: ???
        :param keys: ??????Tuple
        :return:
        """
        await self.__rc.hdel(name, *keys)

    async def lrange(self, name, start, end):
        """
        ??????list?????????????????????
        :param name:
        :param start:
        :param end:
        :return:
        """
        return await self.__rc.lrange(name, start, end)

    async def lpush(self, name, *vals):
        """
        ????????????????????????
        :param name: ???
        :param vals: ?????????
        :return:
        """
        await self.__rc.lpush(name, *vals)

    async def rpush(self, name, *values):
        """
        ????????????????????????
        :param name:
        :param values:
        :return:
        """
        await self.__rc.rpush(name, *values)

    async def rpop(self, name):
        """
        ????????????????????? name ????????????
        :param name:
        :return: ????????????????????? ??? name ????????????????????? nil
        """
        return await self.__rc.rpop(name)

    async def lget(self, name, index):
        """
        ?????????????????????
        :param name: ???
        :param index: ?????????
        :return:
        """
        return await self.__rc.lindex(name, index)

    async def ldel(self, name, value):
        """
        ?????????
        :param name: ???
        :param value: ???
        :return:
        """
        await self.__rc.lrem(name, 1, value)

    async def sadd(self, name, vals):
        """
        ????????????????????????
        :param name: ???
        :param vals: ?????????
        :return:
        """
        await self.__rc.sadd(name, vals)

    async def slen(self, name):
        """
        ????????????????????????
        :param name: ???
        :return: ????????????
        """
        return await self.__rc.scard(name)

    async def smembers(self, name):
        """
        ????????????????????????
        :param name: ???
        :return: ????????????
        """
        return await self.__rc.smembers(name)

    async def sismember(self, name, value):
        """
        ?????????????????????????????????
        :param name: ???
        :param value: ???
        :return:
        """
        return await self.__rc.sismember(name, value)

    async def sdel(self, name):
        """
        ????????????????????????
        :param name:
        :return:
        """
        await self.__rc.spop(name)

    async def set_expire(self, name, seconds: int = settings.REDIS_CACHED_TIMEOUT):
        """
        ?????????????????????
        :param name:
        :param seconds:
        :return:
        """
        if name and seconds:
            return await self.__rc.expire(name, seconds)

    async def set_expire_dt(self, name, dt: datetime.datetime):
        """
        ?????????????????????
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
        ????????????????????????
        :param script_name: ????????????
        :param keys: ??????????????????key1, key2...???
        :param args: ??????????????????key1, key2...???
        :return:
        """
        if script_name in self.LuaDict:
            return await self.LuaDict[script_name].run_script(keys, args)
        else:
            raise Exception(u"????????????????????????")

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
        ??????????????????
        :param transaction:
        :param shard_hint:
        :return:
        """
        return self.__rc.pipeline()

    def transaction(self, watches=None, shard_hint=None):
        """
        ????????????
        :param watches: ??????watch???key??????
        :param shard_hint:
        """
        return self.__rc.pipeline(True, watches=watches, shard_hint=shard_hint)

    def set(self, name, value=None, timeout=settings.REDIS_CACHED_TIMEOUT, existed=None):
        """
        ?????????
        :param name: ???
        :param value: ???
        :param timeout: ???????????????None ???????????????
        :param existed: True ????????????????????? False ????????????????????? None ?????????????????????
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
        ??????name????????????????????????????????????????????????value
        :param name: ???
        :param value: ???
        :param timeout: ???????????????None ???????????????
        :return: True: ????????????
                 False: ?????????
        """
        if value is None:
            value = ''
        if 0 == self.__rc.set(name, value, ex=timeout, nx=True):
            return False
        return True

    def get(self, name):
        """
        ?????????
        :param name: ???
        :return: ???Redis???????????????
        """
        return self.__rc.get(name)

    def exists(self, name):
        """
        ????????????name??????????????????
        :param name:
        :return:
        """
        return self.__rc.exists(name)

    def delete(self, name):
        """
        ?????????
        :param name: ???
        :return:
        """
        self.__rc.delete(name)

    def incr(self, name, amount=1):
        """
        ??????
        :param name: ???
        :param amount: ??????
        :return:
        """
        return self.__rc.incr(name, amount)

    def mset(self, **kwargs):
        """
        ????????????
        :param kwargs: ???????????????key-value???
        :return:
        """
        self.__rc.mset(**kwargs)

    def mget(self, names):
        """
        ???????????????
        :param keys: ????????????List???Tuple
        :return: ?????????????????? List???Tuple????????????????????????
        """
        return self.__rc.mget(names)

    def hset(self, name, key, value=None):
        """
        ????????????dict????????????
        :param name: ???
        :param key: ??????
        :param value: ???
        :return:
        """
        self.__rc.hset(name, key, value)

    def hget(self, name, key):
        """
        ????????????dict????????????
        :param name: ???
        :param key: ??????
        :return:
        """
        return self.__rc.hget(name, key)

    def hmset(self, name, kv_dict=None):
        """
        ????????????dict????????????
        :param name: ???
        :param kv_dict: ???(dict)
        :return:
        """
        self.__rc.hmset(name, kv_dict)

    def hmget(self, name, keys):
        """
        ????????????dict????????????
        :param name: ???
        :param keys: ??????List???Tuple????????????????????????
        :return:
        """
        return self.__rc.hmget(name, keys)

    def hgetall(self, name):
        """
        ??????dict??????????????????
        :param name:
        :return:
        """
        return self.__rc.hgetall(name)

    def hlen(self, name):
        """
        ??????dict??????????????????
        :param name: ???
        :return:
        """
        return self.__rc.hlen(name)

    def hkeys(self, name):
        """
        ??????dict??????????????????
        :param name: ???
        :return:
        """
        return self.__rc.hkeys(name)

    def hvals(self, name):
        """
        ??????dict??????????????????
        :param name: ???
        :return:
        """
        return self.__rc.hvals(name)

    def hexists(self, name, key):
        """
        ????????????????????????
        :param name:
        :param key:
        :return:
        """
        return self.__rc.hexists(name, key)

    def hdel(self, name, *keys):
        """
        ??????dict????????????key
        :param name: ???
        :param keys: ??????Tuple
        :return:
        """
        self.__rc.hdel(name, *keys)

    def lrange(self, name, start, end):
        """
        ??????list?????????????????????
        :param name:
        :param start:
        :param end:
        :return:
        """
        return self.__rc.lrange(name, start, end)

    def lpush(self, name, *vals):
        """
        ????????????????????????
        :param name: ???
        :param vals: ?????????
        :return:
        """
        self.__rc.lpush(name, *vals)

    def rpush(self, name, *values):
        """
        ????????????????????????
        :param name:
        :param values:
        :return:
        """
        self.__rc.rpush(name, *values)

    def rpop(self, name):
        """
        ????????????????????? name ????????????
        :param name:
        :return: ????????????????????? ??? name ????????????????????? nil
        """
        return self.__rc.rpop(name)

    def lget(self, name, index):
        """
        ?????????????????????
        :param name: ???
        :param index: ?????????
        :return:
        """
        return self.__rc.lindex(name, index)

    def ldel(self, name, value):
        """
        ?????????
        :param name: ???
        :param value: ???
        :return:
        """
        self.__rc.lrem(name, 1, value)

    def sadd(self, name, vals):
        """
        ????????????????????????
        :param name: ???
        :param vals: ?????????
        :return:
        """
        self.__rc.sadd(name, vals)

    def slen(self, name):
        """
        ????????????????????????
        :param name: ???
        :return: ????????????
        """
        return self.__rc.scard(name)

    def smembers(self, name):
        """
        ????????????????????????
        :param name: ???
        :return: ????????????
        """
        return self.__rc.smembers(name)

    def sismember(self, name, value):
        """
        ?????????????????????????????????
        :param name: ???
        :param value: ???
        :return:
        """
        return self.__rc.sismember(name, value)

    def sdel(self, name):
        """
        ????????????????????????
        :param name:
        :return:
        """
        self.__rc.spop(name)

    def set_expire(self, name, seconds: int = settings.REDIS_CACHED_TIMEOUT):
        """
        ?????????????????????
        :param name:
        :param seconds:
        :return:
        """
        if name and seconds:
            return self.__rc.expire(name, seconds)

    def set_expire_dt(self, name, dt: datetime.datetime):
        """
        ?????????????????????
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
        ????????????????????????
        :param script_name: ????????????
        :param keys: ??????????????????key1, key2...???
        :param args: ??????????????????key1, key2...???
        :return:
        """
        if script_name in self.LuaDict:
            return self.LuaDict[script_name].run_script(keys, args)
        else:
            raise Exception(u"????????????????????????")

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
