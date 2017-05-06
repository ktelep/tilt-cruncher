from flask import Flask, jsonify, request
from flask import make_response, current_app
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
from datetime import timedelta
from functools import update_wrapper

logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__, static_url_path='/static')

job_schedule = 1

class Config(object):
    JOBS = [
        {
            'id': 'per_devid_stats',
            'func': 'tilt_cruncher:per_devid_stats',
            'trigger': 'interval',
            'seconds': job_schedule
            },
        {
            'id': 'industry',
            'func': 'tilt_cruncher:industry_counts',
            'trigger': 'interval',
            'seconds': job_schedule
            },
        {
            'id': 'hand_check',
            'func': 'tilt_cruncher:hand_check',
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


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator


def timestamp():
    now = time.time()
    localtime = time.localtime(now)
    milliseconds = '%03d' % int((now - int(now)) * 1000)
    return int(time.strftime('%Y%m%d%H%M%S', localtime) + milliseconds)

def hand_check():
    """ Calculates the hands (left or right) """
    time.sleep(0.25)
    start_time = timeit.default_timer()
    logging.info("Beginning hand check")

    device_keys = r.keys('devidhistory:*Hand')
   
    hands = dict()
    hands["Left"] = 0
    hands["Right"] = 0

    hand_data = r.mget(device_keys)
    for hand_val in hand_data:
        if hand_val == "Left":
            hands["Left"] = hands["Left"] + 1
        else:
            hands["Right"] = hands["Right"] + 1

    logging.info(hands)
    r.set("hand:right:history", hands["Right"])
    r.set("hand:left:history", hands["Left"])

def industry_counts():
  
    ind_dict = defaultdict(list)

    device_keys = r.keys('devidhistory:*industry*') 

    # Sort out which keys are for each industry
    industry_data = r.mget(device_keys)
    for i in range(len(industry_data)):
       if industry_data[i] == "":
           continue
       else:
           devid = device_keys[i].split(':')[1]
           ind_dict[industry_data[i]].append(devid) 
 
    shake_variance_key = "devidhistory:%s:TiltLR:Variance"

    for i in ind_dict.keys():
       keys_to_ret = [shake_variance_key % (j,) for j in ind_dict[i]]
       shake_vals = [float(x) for x in r.mget(keys_to_ret)]
       shake_avg = sum(shake_vals)/len(shake_vals)
       r.set("industry:%s:shakeavg" % (i,),shake_avg)
       
    
def calc_variance(mylist):
    
    avg = sum(mylist)/float(len(mylist))
    temp = 0
    for i in range(len(mylist)):
        temp += (mylist[i] - avg) * (mylist[i] - avg) 
        return temp / len(mylist);

def per_devid_stats():
    """ Calculate Min, Max, and Average values of the current active devids """

    start_time = timeit.default_timer()
    logging.info("Beginning average calculation")

    average_vals = ['TiltFB','TiltLR','Direction']
    device_keys = [x.split(':')[-1] for x in r.keys('devid:*')]

    to_insert = dict()

    for key in device_keys:
        for val in average_vals:
            val_key = "devidhistory:%s:%s:Values" % (key, val)
            avg_key = "devidhistory:%s:%s:Average" % (key, val)
            min_key = "devidhistory:%s:%s:Min" % (key, val)
            max_key = "devidhistory:%s:%s:Max" % (key, val)
            var_key = "devidhistory:%s:%s:Variance" % (key, val)   

            hand = "devidhistory:%s:Hand" % (key)
             
            # Need to cast our vals from string to floats
            values = [float(x) for x in r.zrange(val_key,0,-1)]
            total = sum(values)
            avg = total/float(len(values))
            variance = None
 
            # Some stats on the last 25 samples
            if len(values) < 25:
                variance = calc_variance(values)
            else:
                variance = calc_variance(values[-25:])

            logging.info("Variance - %d" % (variance,))
            to_insert[var_key] = variance     

            if "TiltLR" in val:  # Basic hand analysis, based on last 25 data points
                if len(values) < 25:   
                    hand_avg = avg
                else:
                    hand_avg = sum(values[-25:] )/float(25)
                logging.info("hand_avg - %d" % (hand_avg))
                if hand_avg < 0:  # Right Handheld
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
        if not r.exists("devidcount:%s" % (top_score)):

            # Get Count between top and bottom
            count = r.zcount("devidlist", bottom_score, top_score)
            logging.info('%d between %s and %s' % (count, bottom_score, top_score))
            r.set('devidcount:%s' % (top_score), count)

        #Reset the top to the bottom
        top = bottom
        
    elapsed = timeit.default_timer() - start_time
    logging.info("Calculated Devid Counts in %s" % (str(elapsed)))

@app.route('/tilt_hour')
@app.route('/tilt_hour/<hours>')
@crossdomain('*')
def tilt_hour(hours=None):
    keys = r.keys('devidcount:*')
 
    res = dict()
    if keys:
        keys.sort()
        if hours:
             return_keys = keys[-int(hours):]
        else:
             return_keys = keys

        for i in return_keys:
            res[i.split(':')[1]]=int(r.get(i))

    return jsonify(data=res)


@app.route('/industry_shake')
@crossdomain('*')
def industry_shake():
    keys = r.keys('industry:*')

    shake_vals = r.mget(keys)
    industries = [x.split(':')[1] for x in keys]
    
    ret = dict(zip(industries,shake_vals))

    return jsonify(data=ret)

@app.route('/')
@crossdomain('*')
def index():
    return "Tilt-Cruncher!"

   
@app.route('/active_variance')
@app.route('/active_variance/<devid>')
@crossdomain('*')
def active_variance(devid=None):
    device_keys = []

    if devid:
       device_keys.append(devid)
    else:
       device_keys = [x.split(':')[-1] for x in r.keys('devid:*')]

    average_vals = ['TiltFB','TiltLR','Direction']

    return_val = defaultdict(dict)

    for key in device_keys:
        for val in average_vals:
            var_key = "devidhistory:%s:%s:Variance" % (key, val)
            average = r.get(var_key)
            return_val[key][val] = average
    
    return jsonify(data=return_val)

@app.route('/active_averages')
@app.route('/active_averages/<devid>')
@crossdomain('*')
def active_averages(devid=None):
    device_keys = []

    if devid:
       device_keys.append(devid)
    else:
       device_keys = [x.split(':')[-1] for x in r.keys('devid:*')]

    average_vals = ['TiltFB','TiltLR','Direction']

    return_val = defaultdict(dict)

    for key in device_keys:
        for val in average_vals:
            avg_key = "devidhistory:%s:%s:Average" % (key, val)
            average = r.get(avg_key)
            return_val[key][val] = average

    
    return jsonify(data=return_val)

@app.route('/tilt_server_stats')
@crossdomain('*')
def tilt_server_stats():
    device_keys = r.keys('server:*')

    total_processed = r.mget(device_keys)
    ret = defaultdict(dict)

    for i in range(len(device_keys)):
        ret['servers'][device_keys[i]] = total_processed[i]

    total = 0
    device_keys = r.keys('devid:*')
    for i in device_keys:
        total = total + int(r.zcount(i,'-inf','inf'))

    ret['active'] = total

    return jsonify(data=ret)


@app.route('/active_latest')
@app.route('/active_latest/<devid>')
@crossdomain('*')
def active_latest(devid=None):
    device_keys = []

    if devid:
       device_keys.append(devid)
    else:
       device_keys = [x.split(':')[-1] for x in r.keys('devid:*')]

    vals = ['TiltFB','TiltLR','Direction']
    static_vals = ['OS','altitude','latitude','longitude',
                   'industry']

    return_val = defaultdict(dict)

    for key in device_keys:
        for val in vals:
            val_key = "devidhistory:%s:%s:Values" % (key, val)
            data = r.zrevrangebyscore(val_key,"+inf","-inf")
            if data:
                return_val[key][val] = data[0]
            else:
                return_val[key][val] = None

        for val in static_vals:
            val_key = "devidhistory:%s:%s:Values" % (key, val)
            data = r.get(val_key)
            return_val[key][val] = data
      
        for val in ["Hand","TotalBW"]:
           val_key = "devidhistory:%s:%s" % (key, val)
           data = r.get(val_key)
           if data:
                return_val[key][val] = data
           else:
                return_val[key][val] = None

   
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
