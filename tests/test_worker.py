from __future__ import print_function

import json
import redis
import time
import traceback
from zmon_worker_monitor.web import main
from mock import MagicMock

def test_check(monkeypatch):
    data = {}

    def blpop(key, timeout):
        print(key, timeout)
        return key, json.dumps({'properties': {'body_encoding': 'nested'}, 'body': {
            'task': 'check_and_notify',
            'args': [{'check_id': 123, 'entity': {'id': '77', 'type': 'test'}, 'command': 'return 1', 'interval': 10}, {}],
            'kwargs': {},
            'timelimit': [90, 60],
            }})

    def lpush(key, val):
        print(key, val)

    redis = MagicMock()
    redis.blpop = blpop
    redis.lpush = lpush

    def exc(*args, **kwargs):
        print(args, kwargs)
        traceback.print_exc()

    monkeypatch.setattr('zmon_worker_monitor.redis_context_manager.RedisConnHandler.get_conn', lambda x: redis)
    monkeypatch.setattr('zmon_worker_monitor.workflow.logger.exception', exc)

    proc = main(['--no-rpc'])
    # TODO: test check
    time.sleep(0.1)
    proc.proc_control.terminate_all_processes()



