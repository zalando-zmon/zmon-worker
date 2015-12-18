from __future__ import print_function

import json
import redis
import time
import traceback
from zmon_worker_monitor.web import main
from mock import MagicMock

def build_redis_queue_item(check_command):
    return {'properties': {'body_encoding': 'nested'},
            'body': {
                 'task': 'check_and_notify',
                 'args': [{'check_id': 123, 'entity': {'id': '77', 'type': 'test'}, 'command': check_command, 'interval': 10}, {}],
                 'kwargs': {},
                 'timelimit': [90, 60],
           }}

def test_check(tmpdir, monkeypatch):
    data = {}

    def blpop(key, timeout):
        assert key == 'zmon:queue:default'
        return key, json.dumps(build_redis_queue_item('"test-result"'))

    def lpush(key, val):
        data[key] = val
        with open(str(tmpdir) + 'data.json', 'w') as fd:
            json.dump(data, fd)

    redis = MagicMock()
    redis.blpop = blpop
    redis.lpush = lpush

    # to help debugging: print any worker exceptions
    def exc(*args, **kwargs):
        print(args, kwargs)
        traceback.print_exc()

    monkeypatch.setattr('zmon_worker_monitor.redis_context_manager.RedisConnHandler.get_conn', lambda x: redis)
    monkeypatch.setattr('zmon_worker_monitor.workflow.logger.exception', exc)

    proc = main(['--no-rpc'])
    # make sure the worker processes get enough time to execute our check
    time.sleep(0.1)
    proc.proc_control.terminate_all_processes()

    with open(str(tmpdir) + 'data.json') as fd:
        data = json.load(fd)
    assert '"value": "test-result"' in data['zmon:checks:123:77']



