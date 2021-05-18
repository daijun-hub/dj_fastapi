# !/usr/bin/python
import types
import asyncio
from functools import wraps

is_coroutine = asyncio.iscoroutinefunction


def to_sync(func):
    if not is_coroutine(func):
        return func

    @wraps(func)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        fut = asyncio.ensure_future(func(*args, **kwargs))
        loop.run_until_complete(asyncio.gather(fut))
        return fut.result()
    return wrapper


if __name__ == '__main__':
    @to_sync
    async def test():
        await asyncio.sleep(0.01)
        return 1
    print(test())
