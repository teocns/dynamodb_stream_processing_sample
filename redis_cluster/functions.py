
import os
from redis_cluster import RedisCluster
from config import RATE_LIMIT_PER_DOMAIN
import time
import json


def crawler_thread_in_queue(crawler_thread):
    domain = crawler_thread.get('domain')
    threadId = crawler_thread.get('threadId')
    crawler_engine = crawler_thread.get('crawler_engine')

    with RedisCluster.get_connection().pipeline(True) as pipe:
        # Register hit for domain rate limiting purposes

        script = None

        with open(os.path.join(os.getcwd(), 'redis_cluster', 'lua_scripts', 'queue_crawler_thread.lua'), 'r') as f:
            script = f.read()

        callable = pipe.register_script(script)

        callable(
            args=[threadId, domain, crawler_engine, json.dumps(crawler_thread), RATE_LIMIT_PER_DOMAIN, int(time.time())], client=pipe)

        pipe.execute()
