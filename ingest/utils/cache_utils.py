
import redis
import json
from decimal import Decimal
import os


def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=0,
    decode_responses=True, 
)

def cache_get(key: str):
    cached = redis_client.get(key)
    if cached:
        try:
            return json.loads(cached)
        except:
            return cached
    return None

def cache_set(key: str, value, ttl_seconds: int):
    redis_client.setex(key, ttl_seconds, json.dumps(convert_decimals(value)))