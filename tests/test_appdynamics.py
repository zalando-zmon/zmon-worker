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


def kwargs_to_params(kwargs):
    return {k.replace('_', '-'): v for k, v in kwargs.items()}


@pytest.fixture(params=[
    (
        {'time_range_type': BEFORE_NOW, 'duration_in_mins': 5},  # input
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]  # output
    ),
    (
        {'time_range_type': BEFORE_TIME, 'duration_in_mins': 5, 'end_time': 112233},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': AFTER_TIME, 'duration_in_mins': 5, 'start_time': 112233},
        [{'name': 'incident', 'severity': 'CRITICAL'}, {'name': 'incident', 'severity': 'WARNING'}]
    ),
    (
        {'time_range_type': BETWEEN_TIMES, 'end_time': 112233, 'start_time': 112233},
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


def assert_client(cli):
    # hack to access __session obj.
    assert (USER, PASS) == cli._AppdynamicsWrapper__session.auth
    assert get_user_agent() == cli._AppdynamicsWrapper__session.headers['User-Agent']
    assert 'json' == cli._AppdynamicsWrapper__session.params['output']


@pytest.fixture(params=[
    {'time_range_type': BEFORE_NOW},
    {'time_range_type': BEFORE_TIME, 'duration_in_mins': 5},
    {'time_range_type': BEFORE_TIME, 'end_time': 112233},
    {'time_range_type': AFTER_TIME, 'duration_in_mins': 5},
    {'time_range_type': AFTER_TIME, 'start_time': 112233},
    {'time_range_type': BETWEEN_TIMES, 'end_time': 112233},
    {'time_range_type': BETWEEN_TIMES, 'start_time': 112233},
    {'time_range_type': BETWEEN_TIMES, 'duration_in_mins': 5},
    {'time_range_type': BEFORE_NOW, 'duration_in_mins': 5, 'severity': 'WRONG'},
])
def fx_invalid_kwargs(request):
    return request.param


def test_appdynamics_healthrule_violations(monkeypatch, fx_violations):
    kwargs, violations = fx_violations

    resp = resp_mock(violations)
    get = requests_mock(resp)

    monkeypatch.setattr('requests.Session.get', get)

    url = 'https://appdynamics'
    application = 'App 1'

    cli = AppdynamicsWrapper(url, username=USER, password=PASS)

    res = cli.healthrule_violations(application, **kwargs)

    assert violations == res
    assert_client(cli)

    params = kwargs_to_params(kwargs)

    get.assert_called_with(cli.healthrule_violations_url(application), params=params)


def test_appdynamics_healthrule_violations_severity(monkeypatch, fx_violations, fx_severity):
    kwargs, violations = fx_violations

    # URL params
    params = kwargs_to_params(kwargs)

    resp = resp_mock(violations)
    get = requests_mock(resp)

    monkeypatch.setattr('requests.Session.get', get)

    url = 'https://appdynamics'
    application = 'App 1'

    cli = AppdynamicsWrapper(url, username=USER, password=PASS)

    res = cli.healthrule_violations(application, severity=fx_severity, **kwargs)

    assert [v for v in violations if v['severity'] == fx_severity] == res
    assert_client(cli)

    get.assert_called_with(cli.healthrule_violations_url(application), params=params)


def test_appdynamics_healthrule_violations_kwargs_error(monkeypatch, fx_invalid_kwargs):
    url = 'https://appdynamics'
    application = 'App 1'

    cli = AppdynamicsWrapper(url, username=USER, password=PASS)
    with pytest.raises(Exception):
        cli.healthrule_violations(application, **fx_invalid_kwargs)


def test_appdynamics_healthrule_violations_errors(monkeypatch, fx_exception):
    ex, raised = fx_exception

    resp = resp_mock(None, failure=True)
    get = requests_mock(resp, failure=ex)

    monkeypatch.setattr('requests.Session.get', get)

    url = 'https://appdynamics'
    application = 'App 1'

    cli = AppdynamicsWrapper(url, username=USER, password=PASS)

    with pytest.raises(raised):
        cli.healthrule_violations(application, time_range_type=BEFORE_NOW, duration_in_mins=5)
