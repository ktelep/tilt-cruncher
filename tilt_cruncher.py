from flask import Flask, jsonify
from flask_sslify import SSLify
from flask_apscheduler import APScheduler
from collections import defaultdict
import os
import sys
import time
import json
import redis
import timeit
import logging
import datetime


logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__, static_url_path='/static')

job_schedule = 10

class Config(object):
    JOBS = [
        {
            'id': 'per_devid_stats',
            'func': 'tilt_cruncher:per_devid_stats',
            'trigger': 'interval',
            'seconds': job_schedule
            },
        {   
            'id': 'dev_id_counts',
            'func': 'tilt_cruncher:dev_id_counts',
            'args' : (6,),
            'trigger' : 'interval',
            'seconds' : job_schedule
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

except redis.ConnectionError:
    print "Unable to connect to a Redis server, check environment"
    sys.exit(1)

def timestamp():
    now = time.time()
    localtime = time.localtime(now)
    milliseconds = '%03d' % int((now - int(now)) * 1000)
    return int(time.strftime('%Y%m%d%H%M%S', localtime) + milliseconds)

def per_devid_stats():
    """ Calculate Min, Max, and Average values of the current active devids """

    start_time = timeit.default_timer()
    logging.info("Beginning average calculation")

    average_vals = ['TiltFB','TiltLR','Direction','ReqSize']
    device_keys = [x.split(':')[-1] for x in r.keys('devid:*')]

    to_insert = dict()

    for key in device_keys:
        for val in average_vals:
            val_key = "devidhistory:%s:%s:Values" % (key, val)
            avg_key = "devidhistory:%s:%s:Average" % (key, val)
            min_key = "devidhistory:%s:%s:Min" % (key, val)
            max_key = "devidhistory:%s:%s:Max" % (key, val)
            hand = "devidhistory:%s:Hand" % (key)
               
            # Need to cast our vals from string to floats
            values = [float(x) for x in r.zrange(val_key,0,-1)]
            avg = sum(values)/float(len(values))

            if "TiltLR" in val:  # Basic hand analysis.  
                if avg < 0:  # Right Handheld
                    to_insert[hand] = "Right"
                else:
                    to_insert[hand] = "Left"

            to_insert[avg_key] = avg
            to_insert[max_key] = max(values)
            to_insert[min_key] = min(values)
   
    if to_insert.keys():
        r.mset(to_insert)

    elapsed = timeit.default_timer() - start_time
    logging.info("Calculated %d keys in %s" % (len(device_keys), str(elapsed)))

def dev_id_counts(hours):
    """ Calculate the hourly number of tilt users """
    
    start_time = timeit.default_timer()
    logging.info("Beginning dev_id_counts")
 
    current_score = timestamp()
    
    top = datetime.datetime.now()
    top_score = top.strftime("%Y%m%d%H0000000")

    # Calc active this hour and set "current" key
    count = r.zcount("devidlist", top_score, current_score)
    r.set('devidcount:current', count) 
    logging.info('%d between %s and %s' % (count, top_score, current_score))

    # Calc previous hours
    for i in range(0,hours):
        # We want hourly chunks of data
        bottom = top - datetime.timedelta(hours=1)

        # Normalize the times to the top of the hours
        top_score = top.strftime("%Y%m%d%H0000000")
        bottom_score = bottom.strftime("%Y%m%d%H0000000")

        # no need to RE-count already counted time periods
        logging.info(r.exists("devidcount:%s" % (top_score)))
        if not r.exists(top_score):

            # Get Count between top and bottom
            count = r.zcount("devidlist", bottom_score, top_score)
            logging.info('%d between %s and %s' % (count, bottom_score, top_score))
            r.set('devidcount:%s' % (top_score), count)

        #Reset the top to the bottom
        top = bottom
        
    elapsed = timeit.default_timer() - start_time
    logging.info("Calculated Devid Counts in %s" % (str(elapsed)))

@app.route('/')
def index():
    return "Tilt-Cruncher!"

   
@app.route('/active_averages')
@app.route('/active_averages/<devid>')
def active_averages(devid=None):
    device_keys = []

    if devid:
       device_keys.append(devid)
    else:
       device_keys = [x.split(':')[-1] for x in r.keys('devid:*')]

    average_vals = ['TiltFB','TiltLR','Direction','ReqSize']

    return_val = defaultdict(dict)

    for key in device_keys:
        for val in average_vals:
            avg_key = "devidhistory:%s:%s:Average" % (key, val)
            average = r.get(avg_key)
            return_val[key][val] = average

    
    return jsonify(data=return_val)

@app.route('/active_latest')
@app.route('/active_latest/<devid>')
def active_latest(devid=None):
    device_keys = []

    if devid:
       device_keys.append(devid)
    else:
       device_keys = [x.split(':')[-1] for x in r.keys('devid:*')]

    vals = ['TiltFB','TiltLR','Direction','ReqSize']
    static_vals = ['OS','altitude','latitude','longitude',
                   'direction','industry']

    return_val = defaultdict(dict)

    for key in device_keys:
        for val in vals:
            val_key = "devidhistory:%s:%s:Values" % (key, val)
            data = r.zrevrangebyscore(val_key,"+inf","-inf")
            return_val[key][val] = data[0]

        for val in static_vals:
            val_key = "devidhistory:%s:%s:Values" % (key, val)
            data = r.get(val_key)
            return_val[key][val] = data
            
        hand_key = "devidHistory:%s:Hand" % (key,)
        data = r.get(hand_key)
        return_val[key]['Hand'] = data
    
    return jsonify(data=return_val)

sslify = SSLify(app)

app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

if __name__ == '__main__':
    app.debug = True
    print "Running on Port: " + port
    app.run(host='0.0.0.0', port=int(port), use_reloader=False)
