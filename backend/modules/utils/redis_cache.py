import json
import logging
import os
import redis.asyncio as redis

log = logging.getLogger(__name__)

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

_redis = None


def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis


async def get_cached_json(key: str):
    r = get_redis()
    try:
        data = await r.get(key)
        if data:
            return json.loads(data)
    except Exception as e:
        log.warning("Redis get error: %s", e)
    return None


async def set_cached_json(key: str, data: dict, ttl: int = 3600):
    r = get_redis()
    try:
        await r.set(key, json.dumps(data), ex=ttl)
    except Exception as e:
        log.warning("Redis set error: %s", e)
