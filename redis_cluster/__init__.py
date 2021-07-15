import pickle
from redis.client import Redis
from config import redis_endpoint

import redis
from config import redis_endpoint
from threading import Lock


class RedisCluster:
    # Singleton

    _instance: 'RedisCluster' = None

    # Instance variables
    connection: Redis
    connection_name: str  # Charlie123
    conn_lock = Lock()

    @staticmethod
    def instance() -> 'RedisCluster':
        if not RedisCluster._instance:
            return RedisCluster.connect()

        return RedisCluster._instance

    @staticmethod
    def get_connection() -> Redis:
        RedisCluster.ensure_connection()
        return RedisCluster.instance().connection

    @staticmethod
    def kill() -> Redis:
        return RedisCluster.instance().connection.close()

    @staticmethod
    def ensure_connection() -> 'RedisCluster':
        if not RedisCluster.is_connected():
            with RedisCluster.conn_lock:
                RedisCluster._instance = RedisCluster()

    @staticmethod
    def is_connected() -> bool:
        if not RedisCluster._instance:
            return False

        # Verify connection
        try:
            return RedisCluster.instance().connection.ping()
        except:
            return False

    @staticmethod
    def get(key: str):
        return RedisCluster.get_connection().get(key)

    def set(key: str, val: str):
        RedisCluster.get_connection().set(key, val)

    ##############################

    def __init__(self) -> None:
        self.connection = None
        try:
            self.connection = redis.Redis.from_url(
                url=redis_endpoint, port=6379, charset="utf-8", decode_responses=True)
            self.connection.ping()
            self.connection_name = 'connection'
            print(f'[REDIS] Connection {self.connection_name} created')
        except Exception as ex:
            print('Failed connecting to redis')
            del self.connection
