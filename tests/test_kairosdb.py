import requests
import pytest

from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.kairosdb import HttpError, ConfigurationError
from zmon_worker_monitor.builtins.plugins.kairosdb import KairosdbWrapper, DATAPOINTS_ENDPOINT

URL = 'http://kairosdb'


@pytest.fixture(params=[
    (
        {'name': 'check1-metric'},
        {'queries': [{'results': [1, 2]}]},
    ),
    (
        {'name': 'check1-metric', 'tags': {'application_id': ['my-app']}},
        {'queries': [{'results': [1, 2, 3]}]},
    ),
    (
        {'name': 'check1-metric', 'aggregators': [{'name': 'sum'}], 'group_by': [{'name': 'tags', 'tags': ['k']}]},
        {'queries': [{'results': [1, 2, 3, 4]}]},
    ),
    (
        {'name': 'check1-metric', 'aggregators': [{'name': 'sum'}], 'start': 2, 'end': 1, 'time_unit': 'hours'},
        {'queries': [{'results': [1, 2, 3, 4, 5, 6, 7, 8]}]},
    ),
    (
        {
            'name': 'check1-metric',
            'aggregators': [{'name': 'sum'}],
            'start_absolute': 1498049043491,
            'end_absolute': 0
        },
        {'queries': [{'results': [1, 2, 3, 4, 5, 6, 7, 8]}]}
    ),
    (
        {'name': 'check1-metric'},
        requests.Timeout(),
    ),
    (
        {'name': 'check1-metric'},
        requests.ConnectionError(),
    )
])
def fx_query(request):
    return request.param


@pytest.fixture(params=[
    (
        {
            'metrics': [
                {'name': 'check1-metric'},
                {'name': 'check2-metric'}
            ]
        },
        {'queries': [{'results': [1, 2]}, {'results': [2, 3]}]},
    ),
    (
        {
            'metrics': [
                {
                    'name': 'check1-metric',
                    'tags': {'application_id': ['my-app']},
                },
                {
                    'name': 'check2-metric',
                    'tags': {'application_id': ['my-app']},
                },
            ]
        },
        {'queries': [{'results': [1, 2, 3]}, {'results': [2, 3]}]},
    ),
    (
        {
            'metrics': [
                {
                    'name': 'check1-metric',
                    'aggregators':  [{'name': 'sum'}],
                    'group_by': [{'name': 'tags', 'tags': ['k']}]
                },
                {
                    'name': 'check2-metric',
                    'aggregators':  [{'name': 'max'}],
                    'group_by': [{'name': 'tags', 'tags': ['foo']}]
                },
            ],
        },
        {'queries': [{'results': [1, 2, 3, 4]}, {'results': [4, 3, 2, 1]}]},
    ),
    (
        {
            'metrics': [
                {
                    'name': 'check1-metric',
                    'aggregators':  [{'name': 'sum'}],
                },
                {
                    'name': 'check2-metric',
                    'aggregators':  [{'name': 'max'}],
                },
            ],
            'start': 2,
            'end': 1,
            'time_unit': 'hours'
        },
        {'queries': [{'results': [1, 2, 3, 4, 5, 6]}, {'results': [6, 5, 4, 3, 2, 1]}]},
    ),
    (
        {
            'metrics': [
                {
                    'name': 'check1-metric',
                    'aggregators':  [{'name': 'sum'}],
                },
                {
                    'name': 'check2-metric',
                    'aggregators':  [{'name': 'max'}],
                },
            ],
            'start_absolute': 1498049043491,
            'end_absolute': 0
        },
        {'queries': [{'results': [1, 2, 3, 4, 5]}, {'results': [5, 4, 3, 2, 1]}]},
    ),
    (
        {
            'metrics': [
                {'name': 'check1-metric'},
                {'name': 'check2-metric'}
            ]
        },
        requests.Timeout(),
    ),
    (
        {
            'metrics': [
                {'name': 'check1-metric'},
                {'name': 'check2-metric'}
            ]
        },
        requests.ConnectionError(),
    )
])
def fx_query_batch(request):
    return request.param


@pytest.fixture(params=[
    (
        {'name': 'check1-metric', 'start': -5},
        ValueError(),
    ),
    (
        {'name': 'check1-metric', 'end': -1},
        ValueError(),
    ),
])
def fx_args_error(request):
    return request.param


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


def get_final_url():

    return URL + '/' + DATAPOINTS_ENDPOINT


def get_query(kwargs):
    start = kwargs.get('start', 5)
    time_unit = kwargs.get('time_unit', 'minutes')
    group_by = kwargs.get('group_by', [])

    q = {'metrics': [{
        'name': kwargs['name'],
        'group_by': group_by
    }]}

    if 'start_absolute' in kwargs:
        q['start_absolute'] = kwargs['start_absolute']
    else:
        q['start_relative'] = {
            'value': start,
            'unit': time_unit
        }

    if 'end_absolute' in kwargs:
        q['end_absolute'] = kwargs['end_absolute']
    else:
        if 'end' in kwargs:
            q['end_relative'] = {
                'value': kwargs['end'],
                'unit': time_unit
            }

    if 'aggregators' in kwargs:
        q['metrics'][0]['aggregators'] = kwargs.get('aggregators')

    if 'tags' in kwargs:
        q['metrics'][0]['tags'] = kwargs.get('tags')

    return q


def get_query_batch(kwargs):
    start = kwargs.get('start', 5)
    time_unit = kwargs.get('time_unit', 'minutes')

    q = {'metrics': kwargs['metrics']}

    if 'start_absolute' in kwargs:
        q['start_absolute'] = kwargs['start_absolute']
    else:
        q['start_relative'] = {
            'value': start,
            'unit': time_unit
        }

    if 'end_absolute' in kwargs:
        q['end_absolute'] = kwargs['end_absolute']
    else:
        if 'end' in kwargs:
            q['end_relative'] = {
                'value': kwargs['end'],
                'unit': time_unit
            }

    return q


def test_kairosdb_query(monkeypatch, fx_query):
    kwargs, res = fx_query

    failure = True if isinstance(res, Exception) else False

    if failure:
        resp = resp_mock(res, failure=True)
        post = requests_mock(resp, failure=res)
    else:
        resp = resp_mock(res)
        post = requests_mock(resp)

    monkeypatch.setattr('requests.Session.post', post)

    cli = KairosdbWrapper(URL)

    q = get_query(kwargs)

    if failure:
        with pytest.raises(HttpError):
            cli.query(**kwargs)
    else:
        result = cli.query(**kwargs)
        assert result == res['queries'][0]

    post.assert_called_with(get_final_url(), json=q)


def test_kairosdb_query_batch(monkeypatch, fx_query_batch):
    kwargs, res = fx_query_batch

    failure = True if isinstance(res, Exception) else False

    if failure:
        resp = resp_mock(res, failure=True)
        post = requests_mock(resp, failure=res)
    else:
        resp = resp_mock(res)
        post = requests_mock(resp)

    monkeypatch.setattr('requests.Session.post', post)

    cli = KairosdbWrapper(URL)

    q = get_query_batch(kwargs)

    if failure:
        with pytest.raises(HttpError):
            cli.query_batch(**kwargs)
    else:
        result = cli.query_batch(**kwargs)
        assert result == res['queries']

    post.assert_called_with(get_final_url(), json=q)


def test_kairosdb_oauth2(monkeypatch):
    token = 123
    get = MagicMock()
    get.return_value = token
    monkeypatch.setattr('tokens.get', get)

    cli = KairosdbWrapper(URL, oauth2=True)

    assert 'Bearer {}'.format(token) == cli._KairosdbWrapper__session.headers['Authorization']


def test_kairosdb_query_error(monkeypatch):
    resp = resp_mock(None, failure=True)
    resp.status_code = 400
    resp.text = 'invalid query!'

    post = requests_mock(resp)
    monkeypatch.setattr('requests.Session.post', post)

    cli = KairosdbWrapper(URL)

    with pytest.raises(Exception) as ex_info:
        cli.query('check-1')

    assert str(resp.status_code) in str(ex_info.value)
    assert resp.text in str(ex_info.value)


def test_kairosdb_args_error(monkeypatch, fx_args_error):
    kwargs, err = fx_args_error

    cli = KairosdbWrapper(URL)

    with pytest.raises(ValueError):
        cli.query(**kwargs)

    with pytest.raises(ConfigurationError):
        KairosdbWrapper(None)
