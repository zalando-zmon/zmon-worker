import eventlog
import json
import requests
import datetime
from eventlog import Event

_target_host = 'localhost'
_target_port = 8081
_enable_http = True

def set_target_host(host='localhost', port='8081'):
    global _target_host, _target_port
    _target_host = host
    _target_port = port

def enable_http(enable=True):
    global _enable_http
    _enable_http = enable

def register_all(events, path=None):
    eventlog.register_all(events, path)

def log(e_id, **kwargs):
    # for now forward everything
    eventlog.log(e_id, **kwargs)

    if not _enable_http:
        return

    now = datetime.datetime.now()

    headers = {'content-type': 'application/json'}

    event = {'typeId': e_id, 'attributes': kwargs, 'time': now.strftime("%Y-%m-%dT%H:%M:%S.")+now.strftime("%f")[:3]}

    try:
        res = requests.put('http://{}:{}/'.format(_target_host, _target_port), data=json.dumps([event]), headers=headers)
    except Exception as e:
        pass

