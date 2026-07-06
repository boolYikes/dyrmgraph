from contextlib import contextmanager
from os import environ


@contextmanager
def get_redis_client():
    from redis import Redis

    client = Redis(
        host=environ["REDIS_HOST"],
        port=int(environ["REDIS_PORT"]),
        decode_responses=True,
    )
    try:
        yield client
    finally:
        client.close()
