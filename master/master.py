from flask import Flask, request, jsonify
from redis import Redis, RedisError
import zmq

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

app  = Flask(__name__)
key  = 'k'
rctx = RedisContext(host="redis", db=0, socket_timeout=5, socket_keepalive=True)

@app.route('/fib', methods=['GET', 'POST'])
def fibbonachi_get():
    global key
    code     = 200
    response = ''

    if request.method == 'GET':
        try:
            with rctx as redis:
                response = int(redis.lindex(key, 1))
        except:
            # TODO: Log the error
            code = 500
    else:
        # By default, 0mq has no-exception policy
        ctx = zmq.Context()
        socket = ctx.socket(zmq.REQ)
        socket.connect('tcp://worker:5555')
        socket.send_string(key)
        if socket.recv_string() != key:
            code = 500
    return jsonify(response), code

if __name__ == "__main__":
    # TODO: Put in GET when key will become part of a request
    try:
        with rctx as redis:
            if redis.exists(key) == 0:
                redis.rpush(key, 1, 1)
    except RedisError:
        raise Exception("Cannot set default value in DB.")
    app.run(host='0.0.0.0', port=80, debug=True) #, threaded=True)
