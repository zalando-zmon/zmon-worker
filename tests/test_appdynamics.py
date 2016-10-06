import pytest
import requests

from mock import MagicMock

from zmon_worker_monitor.zmon_worker.errors import HttpError
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from zmon_worker_monitor.builtins.plugins.appdynamics import CRITICAL, WARNING
from zmon_worker_monitor.builtins.plugins.appdynamics import BEFORE_TIME, BEFORE_NOW, AFTER_TIME, BETWEEN_TIMES
from zmon_worker_monitor.builtins.plugins.appdynamics import AppdynamicsWrapper


USER = 'user'
PASS = 'pass'
URL = 'https://appdynamics'
APPLICATION = 'App 1'


def resp_mock(res, failure=False):
    resp = MagicMock()
    resp.ok = True if not failure else False
    resp.json.return_value = res

    return resp


def requests_mock(resp, failure=None):
    req = MagicMock()

    if failure is not None:
        req.side_effect = failure
    else:
        req.return_value = resp

    return req


def kwargs_to_params(kwargs, start_time, end_time):
    res = {k.replace('_', '-'): v for k, v in kwargs.items()}

    if not kwargs:
        res['time-range-type'] = BEFORE_NOW

    if 'duration-in-mins' not in res and res['time-range-type'] in (BEFORE_NOW, AFTER_TIME, BEFORE_TIME):
        res['duration-in-mins'] = 5

    if 'start-time' not in res and res['time-range-type'] in (AFTER_TIME, BETWEEN_TIMES):
        res['start-time'] = start_time * 1000

    if 'end-time' not in res and res['time-range-type'] in (BEFORE_TIME, BETWEEN_TIMES):
        res['end-time'] = end_time * 1000

    return res


@pytest.fixture(params=[
    (
        {'time_range_type': BEFORE_NOW, 'duration_in_mins': 5},  # input
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]  # output
    ),
    (
        {},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': BEFORE_NOW},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': BEFORE_TIME, 'duration_in_mins': 5, 'end_time': 112233},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': BEFORE_TIME},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': AFTER_TIME, 'duration_in_mins': 5, 'start_time': 112233},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': AFTER_TIME},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': BETWEEN_TIMES, 'end_time': 112233, 'start_time': 112233},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': BETWEEN_TIMES},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    )
])
def fx_violations(request):
    return request.param


@pytest.fixture(params=[CRITICAL, WARNING])
def fx_severity(request):
    return request.param


@pytest.fixture(params=[
    (requests.ConnectionError, HttpError),  # (req exception, raised exception)
    (requests.Timeout, HttpError),
    (RuntimeError, RuntimeError)
])
def fx_exception(request):
    return request.param


@pytest.fixture(params=[
    ('https://es-url', {'q': 'application_id:my-app'}, {'hits': {'hits': ['res1', 'res2']}}),
    ('https://es-url', {'q': 'application_id:my-app AND message:[1-9]{2}'}, {'hits': {'hits': ['res1', 'res2']}}),
    ('https://es-url', {'body': {'query': {'query_str': 'my-app'}}}, {'hits': {'hits': ['res1']}}),
    ('https://es-url', {'body': {'query': {'query_str': 'my-app'}}, 'raw_result': True}, {'hits': {'hits': ['res1']}}),
])
def fx_log_hits(request):
    return request.param


@pytest.fixture(params=[
    ('https://es-url', {'q': 'application_id:my-app'}, {'count': 2}),
    ('https://es-url', {'q': 'application_id:my-app AND message:[1-9]{2}'}, {'count': 3}),
    ('https://es-url', {'body': {'query': {'query_str': 'my-app'}}}, {'count': 1}),
])
def fx_log_count(request):
    return request.param


@pytest.fixture(params=[
    {'time_range_type': BEFORE_NOW, 'duration_in_mins': None},
    {'time_range_type': BEFORE_NOW, 'duration_in_mins': 5, 'severity': 'WRONG'},
    {'time_range_type': 'WRONG', 'duration_in_mins': 5},
])
def fx_invalid_kwargs(request):
    return request.param


@pytest.fixture(params=[
    {'time_range_type': BEFORE_NOW, 'duration_in_mins': None, 'start_time': None, 'end_time': None},
    {'time_range_type': '_INVALID', 'duration_in_mins': 5, 'start_time': None, 'end_time': None},
])
def fx_invalid_time_range_params(request):
    return request.param


@pytest.fixture(params=[
    (
        {'time_range_type': AFTER_TIME, 'duration_in_mins': 5, 'start_time': None, 'end_time': None},
        {'start-time': 123, 'end-time': 456},
        {'duration-in-mins': 5, 'start-time': 123000, 'time-range-type': AFTER_TIME}
    ),
    (
        {'time_range_type': BEFORE_TIME, 'duration_in_mins': 5, 'start_time': None, 'end_time': None},
        {'start-time': 123, 'end-time': 456},
        {'duration-in-mins': 5, 'end-time': 456000, 'time-range-type': BEFORE_TIME}
     ),
    (
        {'time_range_type': BETWEEN_TIMES, 'duration_in_mins': 5, 'start_time': None, 'end_time': None},
        {'start-time': 123, 'end-time': 456},
        {'end-time': 456000, 'start-time': 123000, 'time-range-type': BETWEEN_TIMES}
     )
])
def fx_valid_time_range_params(request):
    return request.param


@pytest.fixture(params=[
    (
      {'metric_path': 'P_'},
      {'time-range-type': BEFORE_NOW, 'rollup': True, 'duration-in-mins': 5, 'metric-path': 'P_'}
    ),
    (
        {'metric_path': 'P_', 'rollup': False, 'duration_in_mins': 10, 'time_range_type': BEFORE_TIME, 'end_time': 123},
        {'metric-path': 'P_', 'rollup': False, 'duration-in-mins': 10, 'time-range-type': BEFORE_TIME, 'end-time': 123},
    )
])
def fx_data_metric(request):
    return request.param


def assert_client(cli):
    # hack to access __session obj.
    assert (USER, PASS) == cli._AppdynamicsWrapper__session.auth
    assert get_user_agent() == cli._AppdynamicsWrapper__session.headers['User-Agent']
    assert 'json' == cli._AppdynamicsWrapper__session.params['output']


def test_prepare_time_range_params_with_exception(fx_invalid_time_range_params):
    cli = AppdynamicsWrapper(URL, USER, PASS)
    with pytest.raises(Exception):
        cli._prepare_time_range_params(**fx_invalid_time_range_params)


def test_prepare_time_range_params(monkeypatch, fx_valid_time_range_params):
    kwargs, mock_time_vals, result = fx_valid_time_range_params

    cli = AppdynamicsWrapper(URL, USER, PASS)

    start_time = mock_time_vals['start-time']
    end_time = mock_time_vals['end-time']

    mktime_mock = MagicMock()
    time_mock = MagicMock()
    mktime_mock.return_value = start_time
    time_mock.return_value = end_time
    monkeypatch.setattr('time.mktime', mktime_mock)
    monkeypatch.setattr('time.time', time_mock)

    assert cli._prepare_time_range_params(**kwargs) == result


def test_appdynamics_data_metric(monkeypatch, fx_data_metric):
    url_params, params = fx_data_metric

    res_json = []
    resp = resp_mock([])
    get = requests_mock(resp)
    monkeypatch.setattr('requests.Session.get', get)

    cli = AppdynamicsWrapper(URL, USER, PASS)
    res = cli.metric_data(APPLICATION, **url_params)
    assert res == res_json

    assert_client(cli)
    get.assert_called_with(cli.metric_data_url(APPLICATION), params=params)


def test_appdynamics_data_metric_missing_metric_path():
    cli = AppdynamicsWrapper(URL, USER, PASS)
    with pytest.raises(Exception):
        cli.metric_data(APPLICATION, None)


def test_appdynamics_healthrule_violations(monkeypatch, fx_violations):
    kwargs, violations = fx_violations

    resp = resp_mock(violations)
    get = requests_mock(resp)

    monkeypatch.setattr('requests.Session.get', get)

    start_time = 12345
    end_time = 23456

    mktime_mock = MagicMock()
    time_mock = MagicMock()
    mktime_mock.return_value = start_time
    time_mock.return_value = end_time
    monkeypatch.setattr('time.mktime', mktime_mock)
    monkeypatch.setattr('time.time', time_mock)

    cli = AppdynamicsWrapper(URL, username=USER, password=PASS)

    res = cli.healthrule_violations(APPLICATION, **kwargs)

    assert violations == res
    assert_client(cli)

    params = kwargs_to_params(kwargs, start_time, end_time)

    get.assert_called_with(cli.healthrule_violations_url(APPLICATION), params=params)


def test_appdynamics_healthrule_violations_severity(monkeypatch, fx_violations, fx_severity):
    kwargs, violations = fx_violations

    resp = resp_mock(violations)
    get = requests_mock(resp)

    monkeypatch.setattr('requests.Session.get', get)

    start_time = 12345
    end_time = 23456

    mktime_mock = MagicMock()
    time_mock = MagicMock()
    mktime_mock.return_value = start_time
    time_mock.return_value = end_time
    monkeypatch.setattr('time.mktime', mktime_mock)
    monkeypatch.setattr('time.time', time_mock)

    cli = AppdynamicsWrapper(URL, username=USER, password=PASS)

    res = cli.healthrule_violations(APPLICATION, severity=fx_severity, **kwargs)

    assert [v for v in violations if v['severity'] == fx_severity] == res
    assert_client(cli)

    params = kwargs_to_params(kwargs, start_time, end_time)

    get.assert_called_with(cli.healthrule_violations_url(APPLICATION), params=params)


def test_appdynamics_oauth2(monkeypatch):
    # mock tokens
    token = 'TOKEN-123'
    monkeypatch.setattr('tokens.get', lambda x: token)

    cli = AppdynamicsWrapper(url=URL)

    assert 'Bearer {}'.format(token) == cli._AppdynamicsWrapper__session.headers['Authorization']
    assert True is cli._AppdynamicsWrapper__oauth2


def test_appdynamics_healthrule_violations_kwargs_error(fx_invalid_kwargs):
    cli = AppdynamicsWrapper(URL, username=USER, password=PASS)
    with pytest.raises(Exception):
        cli.healthrule_violations(APPLICATION, **fx_invalid_kwargs)


def test_appdynamics_healthrule_violations_errors(monkeypatch, fx_exception):
    ex, raised = fx_exception

    resp = resp_mock(None, failure=True)
    get = requests_mock(resp, failure=ex)

    monkeypatch.setattr('requests.Session.get', get)

    cli = AppdynamicsWrapper(URL, username=USER, password=PASS)

    with pytest.raises(raised):
        cli.healthrule_violations(APPLICATION, time_range_type=BEFORE_NOW, duration_in_mins=5)


def test_appdynamics_metric_data_errors(monkeypatch, fx_exception):
    ex, raised = fx_exception

    resp = resp_mock(None, failure=True)
    get = requests_mock(resp, failure=ex)

    monkeypatch.setattr('requests.Session.get', get)

    metric_path = 'Some | Metric | Path'
    cli = AppdynamicsWrapper(URL, username=USER, password=PASS)

    with pytest.raises(raised):
        cli.metric_data(APPLICATION, metric_path=metric_path)


def test_appdynamics_no_url():
    with pytest.raises(RuntimeError):
        AppdynamicsWrapper()


def test_appdynamics_log_query(monkeypatch, fx_log_hits):
    es_url, kwargs, res = fx_log_hits

    search = MagicMock()
    search.return_value = res

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.appdynamics.ElasticsearchWrapper.search', search)

    timestamp = 1234
    timestamp_mock = MagicMock()
    timestamp_mock.return_value = timestamp
    monkeypatch.setattr(AppdynamicsWrapper, '_AppdynamicsWrapper__get_timestamp', timestamp_mock)

    cli = AppdynamicsWrapper(url=URL, es_url=es_url, username=USER, password=PASS, index_prefix='PREFIX_')

    exp_q = ('{} AND eventTimestamp:>{}'
             .format(kwargs.get('q', ''), timestamp)
             .lstrip(' AND '))

    result = cli.query_logs(**kwargs)

    expected = res['hits']['hits'] if not kwargs.get('raw_result', False) else res

    assert result == expected

    kwargs.pop('raw_result', None)
    kwargs['q'] = exp_q
    kwargs['size'] = 100
    kwargs['indices'] = ['PREFIX_*']
    if 'body' not in kwargs:
        kwargs['body'] = None

    search.assert_called_with(**kwargs)


def test_appdynamics_log_count(monkeypatch, fx_log_count):
    es_url, kwargs, res = fx_log_count

    count = MagicMock()
    count.return_value = res

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.appdynamics.ElasticsearchWrapper.count', count)

    timestamp = 1234
    timestamp_mock = MagicMock()
    timestamp_mock.return_value = timestamp
    monkeypatch.setattr(AppdynamicsWrapper, '_AppdynamicsWrapper__get_timestamp', timestamp_mock)

    cli = AppdynamicsWrapper(url=URL, es_url=es_url, username=USER, password=PASS, index_prefix='PREFIX_')

    exp_q = ('{} AND eventTimestamp:>{}'
             .format(kwargs.get('q', ''), timestamp)
             .lstrip(' AND '))

    result = cli.count_logs(**kwargs)

    assert result == res['count']

    kwargs['q'] = exp_q
    kwargs['indices'] = ['PREFIX_*']
    if 'body' not in kwargs:
        kwargs['body'] = None

    count.assert_called_with(**kwargs)


def test_appdynamics_log_error(monkeypatch):
    cli = AppdynamicsWrapper(url=URL, username=USER, password=PASS)

    with pytest.raises(RuntimeError):
        cli.query_logs(q='application:my-app')

    with pytest.raises(RuntimeError):
        cli.count_logs(q='application:my-app')
