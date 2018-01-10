import zmq
from redis import Redis
import signal
import sys

def handler(*args, **kwargs):
    sys.exit(0)

class RedisContext(object):
    def __init__(self, *args, **kwargs):
        self.args   = args
        self.kwargs = kwargs
        self.redis  = None

    def __enter__(self):
        # XXX: ping() may throw exception on reconnect
        if not (self.redis and self.redis.ping()):
            self.redis = Redis(*self.args, **self.kwargs)
        return self.redis

    def __exit__(self, *args, **kwargs):
        pass


signal.signal(signal.SIGTERM, handler)
rctx = RedisContext(host="redis", db=0, socket_timeout=5)
ctx  = zmq.Context()
socket = ctx.socket(zmq.REP)
socket.bind('tcp://*:5555')

while True:
    key = socket.recv()
    replied = False
    try:
        with rctx as redis:
            socket.send(key)
            replied = True

            with redis.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(key)
                        data = pipe.lrange(key, 0, -1)
                        next_ = int(data[0]) + int(data[1])
                        pipe.lset(key, 0, data[1])
                        pipe.lset(key, 1, next_)
                        pipe.execute()
                        break
                    except WatchError:
                        continue
    except RedisError:
        if not replied:
            socket.send('')

