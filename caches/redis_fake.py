#!/usr/bin/python

import functools
from fakeredis import FakeStrictRedis
from commons.coroutine_utils import is_coroutine
import redis

if int(redis.__version__.split(".")[0]) >= 3:
    from redis.client import Pipeline
else:
    from redis.client import BasePipeline as Pipeline


class AsyncCommand(object):
    pass


def to_async(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


class AsyncMetaClass(type):
    def __new__(mcs, name, bases, attributes):
        for key, val in attributes.items():
            if isinstance(val, AsyncCommand):
                for base in bases:
                    val = getattr(base, key, None)
                    if not callable(val):
                        continue
                    async_val = to_async(val)
                    if async_val:
                        attributes[key] = async_val
                        break
        new_class = super(AsyncMetaClass, mcs).__new__(
            mcs, name, bases, attributes)
        return new_class


class AsyncFakeStrictRedis(FakeStrictRedis, metaclass=AsyncMetaClass):
    get = AsyncCommand()
    set = AsyncCommand()
    setnx = AsyncCommand()
    exists = AsyncCommand()
    delete = AsyncCommand()
    incr = AsyncCommand()
    mset = AsyncCommand()
    mget = AsyncCommand()
    hset = AsyncCommand()
    hsetnx = AsyncCommand()
    hget = AsyncCommand()
    hmget = AsyncCommand()
    hmset = AsyncCommand()
    hgetall = AsyncCommand()
    hlen = AsyncCommand()
    hkeys = AsyncCommand()
    hvals = AsyncCommand()
    hexists = AsyncCommand()
    hdel = AsyncCommand()
    hrange = AsyncCommand()
    lpush = AsyncCommand()
    rpush = AsyncCommand()
    rpop = AsyncCommand()
    lpop = AsyncCommand()
    lindex = AsyncCommand()
    ldel = AsyncCommand()
    lrange = AsyncCommand()
    sadd = AsyncCommand()
    slen = AsyncCommand()
    smembers = AsyncCommand()
    sismember = AsyncCommand()
    srem = AsyncCommand()
    sdel = AsyncCommand()
    sunionstore = AsyncCommand()
    sdiffstore = AsyncCommand()
    spop = AsyncCommand()
    expire = AsyncCommand()
    expireat = AsyncCommand()
    register_script = AsyncCommand()
    evalsha = AsyncCommand()
    flushall = AsyncCommand()

    def pipeline(self, transaction=True, shard_hint=None):
        pipeline = AsyncStrictPipeline(self.connection_pool, self.response_callbacks,
                                       transaction, shard_hint)
        # await pipeline.reset()
        pipeline.command_stack = []
        pipeline.scripts = set()
        pipeline.explicit_transaction = False
        return pipeline


class AsyncStrictPipeline(Pipeline, AsyncFakeStrictRedis, metaclass=AsyncMetaClass):
    watch = AsyncCommand()
    unwatch = AsyncCommand()
    execute = AsyncCommand()
    load_script = AsyncCommand()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.reset()
