import eventlog
import json
import requests
import datetime

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

    event = {'typeId': e_id, 'attributes': kwargs, 'time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}
    try:
        requests.put('http://{}:{}/'.format(_target_host, _target_port), data=[json.dumps(event)])
    except Exception as e:
        pass

