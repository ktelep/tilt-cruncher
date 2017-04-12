from flask import Flask
from flask_sslify import SSLify
from flask_apscheduler import APScheduler
import os
import time
import json
import redis
import logging


logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__, static_url_path='/static')

class Config(object):
    JOBS = [
        {
            'id': 'average',
            'func': 'tilt_cruncher:average',
            'args': (1, 2),
            'trigger': 'interval',
            'seconds': 10 
            }
    ]
    SCHEDULER_API_ENABLED = True




# Determine our running port.  Updated to support Diego which uses the
# 'PORT' environment variable, vs non-Diego which uses VCAP_APP_PORT

if os.getenv('VCAP_APP_PORT'):
    port = os.getenv('VCAP_APP_PORT')
elif os.getenv('PORT'):
    port = os.getenv('PORT')
else:
    port = "8080"


if os.getenv('CF_INSTANCE_INDEX'):
    inst_index = os.getenv('CF_INSTANCE_INDEX')
else:
    inst_index = "UNK"

redis_keys = {
                'pivotalcf': {
                                'service': 'rediscloud',
                                'host': 'hostname',
                                'port': 'port',
                                'password': 'password'
                            },
                'pcfdev': {
                                'service': 'p-redis',
                                'host': 'host',
                                'port': 'port',
                                'password': 'password'
                            },
                'bluemix': {
                                'service': 'redis-2.6',
                                'host': 'hostname',
                                'port': 'port',
                                'password': 'password'
                            }
            }



def getServiceInfo():
    redis_service = None
    for value in redis_keys.iteritems():
        # Service Key Name
        service = value[1]['service']

        if service in json.loads(os.environ['VCAP_SERVICES']):
            redis_service = json.loads(os.environ['VCAP_SERVICES'])[service][0]
            break
        else:
            continue

    if redis_service:
        return redis_service
    else:
        raise KeyError("Unable to identify Redis Environment")


def getHostKey():
    hostKey = None
    for value in redis_keys.iteritems():
        # Host Key Name
        host = value[1]['host']
        if host in credentials:
            hostKey = host
            break
        else:
            continue

    if hostKey:
        return hostKey
    else:
        raise KeyError("Unable to identify Redis Host")

# Determine our Application Name
app_name = None

if os.getenv('VCAP_APPLICATION'):
    app_name = json.loads(os.environ['VCAP_APPLICATION'])['application_name']

# Connect to our Redis service in cloudfoundry
if os.getenv('VCAP_SERVICES'):
    redis_service = getServiceInfo()

    credentials = redis_service['credentials']
    hostKey = getHostKey()
    print credentials[hostKey]
    pool = redis.ConnectionPool(host=credentials[hostKey],
                                port=credentials['port'],
                                password=credentials['password'],
                                max_connections=2)

    r = redis.Redis(connection_pool=pool)
else:   # Local redis server as a failback
    r = redis.Redis()

try:
    # Test our connection
    response = r.client_list()

    r.set("server:" + inst_index, 0)
    r.expire("server:" + inst_index, 3)

except redis.ConnectionError:
    print "Unable to connect to a Redis server, check environment"
    sys.exit(1)


def timestamp():
    now = time.time()
    localtime = time.localtime(now)
    milliseconds = '%03d' % int((now - int(now)) * 1000)
    return int(time.strftime('%Y%m%d%H%M%S', localtime) + milliseconds)

def job1(a, b):
    print timestamp()
    r.set("Test",timestamp())

sslify = SSLify(app)

app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

if __name__ == '__main__':
    app.debug = True
    print "Running on Port: " + port
    app.run(host='0.0.0.0', port=int(port), use_reloader=False)
