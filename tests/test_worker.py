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
                 'args': [{'check_id': 123, 'check_name': 'Test Check',
                           'entity': {'id': '77', 'type': 'test'}, 'command': check_command, 'interval': 10},
                        []],
                 'kwargs': {},
                 'timelimit': [90, 60],
           }}


def execute_check(tmpdir, monkeypatch, check_command, expected_strings):
    data = {}

    def get_data():
        try:
            with open(str(tmpdir) + 'data.json') as fd:
                data = json.load(fd)
        except:
            data = None
        return data


    def blpop(key, timeout):
        assert key in ('zmon:queue:default', 'zmon:queue:internal')
        if key == 'zmon:queue:default':
            return key, json.dumps(build_redis_queue_item(check_command))

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

    proc = main(['-c', 'tests/config-test.yaml', '--no-rpc'])
    start = time.time()
    # make sure the worker processes get enough time to execute our check
    # wait up to 5 seconds
    while not get_data() and time.time() < start + 5:
        time.sleep(1.5)
    proc.proc_control.terminate_all_processes()

    data = get_data()
    for string in expected_strings:
        assert string in data['zmon:checks:123:77']


def test_check_failure(tmpdir, monkeypatch):
    execute_check(tmpdir, monkeypatch, 'invalid_python_code', ['"value": "name \'invalid_python_code\' is not defined"', '"exc": 1'])

def test_check_success(tmpdir, monkeypatch):
    execute_check(tmpdir, monkeypatch, '"test-result"', ['"value": "test-result"'])

