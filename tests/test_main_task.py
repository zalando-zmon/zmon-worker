import json

import pytest
import time

from zmon_worker_monitor.zmon_worker.tasks.main import MainTask
from zmon_worker_monitor import plugin_manager

from mock import MagicMock

ONE_DAY = 24 * 3600

def test_check(monkeypatch):
    reload(plugin_manager)
    plugin_manager.init_plugin_manager()  # init plugin manager

    MainTask.configure({})
    task = MainTask()
    monkeypatch.setattr(task, '_get_check_result', MagicMock())
    monkeypatch.setattr(task, '_store_check_result', MagicMock())
    monkeypatch.setattr(task, 'send_metrics', MagicMock())
    req = {'check_id': 123, 'entity': {'id': 'myent'}}
    task.check(req)


def test_evaluate_alert(monkeypatch):
    reload(plugin_manager)
    plugin_manager.init_plugin_manager()  # init plugin manager
    plugin_manager.collect_plugins()

    # mock Redis
    con = MagicMock()
    monkeypatch.setattr(MainTask, 'con', con)
    MainTask.configure({})
    task = MainTask()
    alert_def = {'id': 1, 'check_id': 123, 'condition': '>0', 'parameters': {'p1': {'value': 'x'}}}
    req = {'check_id': 123,
           'entity': {'id': '77', 'type': 'test'}}
    result = {'ts': 10, 'value': 0}
    is_alert, captures = task.evaluate_alert(alert_def, req, result)
    assert {'p1': 'x'} == captures
    assert not is_alert

    # change value over threshold
    result = {'ts': 10, 'value': 1}
    is_alert, captures = task.evaluate_alert(alert_def, req, result)
    assert {'p1': 'x'} == captures
    assert is_alert

    # produce exception
    alert_def['condition'] = 'value["missing-key"] > 0'
    is_alert, captures = task.evaluate_alert(alert_def, req, result)
    assert {'p1': 'x', 'exception': "'int' object has no attribute '__getitem__'"} == captures
    assert is_alert


def test_evaluate_downtimes(monkeypatch):
    downtimes = [{'id': 'dt-active', 'start_time': 0, 'end_time': time.time() + ONE_DAY},
                 {'id': 'dt-expired', 'start_time': 0, 'end_time': 2},
                 {'id': 'dt-future', 'start_time': time.time() + ONE_DAY, 'end_time': time.time() + (2 * ONE_DAY)}]
    downtimes_active = downtimes[:1]  # only the first one is still active

    # mock Redis
    con = MagicMock()
    con.pipeline.return_value.execute.return_value = (['ent1'], {'dt-active': json.dumps(downtimes[0]),
                                                                 'dt-expired': json.dumps(downtimes[1]),
                                                                 'dt-future': json.dumps(downtimes[2])})
    monkeypatch.setattr(MainTask, 'con', con)
    MainTask.configure({})
    task = MainTask()
    result = task._evaluate_downtimes(1, 'ent1')
    assert downtimes_active == result


def test_notify(monkeypatch):
    reload(plugin_manager)
    plugin_manager.init_plugin_manager()  # init plugin manager
    plugin_manager.collect_plugins()

    # mock Redis
    con = MagicMock()
    monkeypatch.setattr(MainTask, 'con', con)
    monkeypatch.setattr(MainTask, '_evaluate_downtimes', lambda self, x, y: [])
    MainTask.configure({})
    task = MainTask()
    alert_def = {'id': 42, 'check_id': 123, 'condition': '>0', 'parameters': {'p1': {'value': 'x'}}}
    req = {'check_id': 123,
           'check_name': 'My Check',
           'entity': {'id': '77', 'type': 'test'}}
    result = {'ts': 10, 'value': 0}
    notify_result = task.notify(result, req, [alert_def])
    assert [] == notify_result

    # 1 > 0 => trigger active alert!
    result = {'ts': 10, 'value': 1}
    notify_result = task.notify(result, req, [alert_def])
    assert [alert_def['id']] == notify_result

    # alert is not in time period
    alert_def['period'] = 'year {1980}'
    result = {'ts': 10, 'value': 1}
    notify_result = task.notify(result, req, [alert_def])
    assert [] == notify_result


def test_send_to_dataservice(monkeypatch):
    check_results = [{'check_id': 123, 'ts': 10, 'value': 'CHECK-VAL'}]
    expected = {'account': 'myacc', 'team': 'myteam', 'results': check_results}

    put = MagicMock()
    monkeypatch.setattr('requests.put', put)
    monkeypatch.setattr('tokens.get', lambda x: 'mytok')

    MainTask.configure({'account': expected['account'], 'team': expected['team'],
                        'dataservice.url': 'https://example.org', 'dataservice.oauth2': True})
    MainTask.send_to_dataservice(check_results)
    args, kwargs = put.call_args
    assert args[0] == 'https://example.org/myacc/123/'
    assert expected == json.loads(kwargs['data'])


@pytest.mark.parametrize('result,expected', [
    ({'ts': 10, 'value': {'a': {'b': 12.25}, 'non-float': 'IGNORE-ME'}},
     [{"tags": {"metric": "b", "key": "a.b", "entity": "77"}, "name": "zmon.check.123",
       "datapoints": [[10000, 12.25]]}]),
    ({'ts': 10, 'value': 7.5}, [{"tags": {"entity": "77"}, "name": "zmon.check.123",
                                 "datapoints": [[10000, 7.5]]}])
])
def test_store_kairosdb(monkeypatch, result, expected):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)
    MainTask.configure({'kairosdb.enabled': True, 'kairosdb.host': 'example.org', 'kairosdb.port': 8080})
    task = MainTask()
    task._store_check_result_to_kairosdb({'check_id': 123,
                                          'entity': {'id': '77', 'type': 'test'}}, result)
    args, kwargs = post.call_args
    assert args[0] == 'http://example.org:8080/api/v1/datapoints'
    # decode JSON again to make the test stable (to not rely on dict key order)
    assert expected == json.loads(args[1])
    assert kwargs == {'timeout': 2}
