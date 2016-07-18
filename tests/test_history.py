import pytest

from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.history import ONE_WEEK, ONE_WEEK_AND_5MIN, DATAPOINTS_ENDPOINT
from zmon_worker_monitor.builtins.plugins.history import HistoryWrapper, ConfigurationError

URL = 'http://kairosdb'

FILTER_KEY = 'my-key'


@pytest.fixture(params=[
    (
        {'entities': ['1', '2'], 'check_id': '1'},  # wrapper args
        {},  # function args
        {'queries': [{'results': [{'values': [(1, 1), (2, 2), (3, 3)], 'tags': {}}]}]},  # result
    ),
    (
        {'entities': ['1', '2'], 'check_id': '1'},
        {'time_from': 1234, 'time_to': 123},
        {'queries': [
            {
                'results': [
                    {
                        'values': [(1, 1), (2, 2), (3, 3)],
                        'tags': {'key': [FILTER_KEY]}
                    }
                ]
            }
        ]},
    ),
    (
        {'entities': ['1', '2'], 'check_id': '1'},
        {'time_from': 1234, 'time_to': 123},
        {'queries': [
            {
                'results': [
                    {
                        'values': [],  # no values!
                        'tags': {'key': [FILTER_KEY]}
                    }
                ]
            }
        ]},
    ),
])
def fx_result(request):
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


def mock_all(monkeypatch, res):
    resp = resp_mock(res)
    post = requests_mock(resp)

    get_request = MagicMock()
    get_request.return_value = {'metrics': {}}

    monkeypatch.setattr('requests.Session.post', post)
    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.history.get_request', get_request)

    return post, get_request


def assert_get_request(get_request, kwargs, wrapper_kwargs, aggregator=None):
    time_from = ONE_WEEK_AND_5MIN if 'time_from' not in kwargs else kwargs['time_from']
    time_to = ONE_WEEK if 'time_to' not in kwargs else kwargs['time_to']

    args = [wrapper_kwargs['check_id'], wrapper_kwargs['entities'], int(time_from), int(time_to)]

    if aggregator:
        args += [aggregator, int(time_from - time_to)]

    get_request.assert_called_with(*args)


def assert_aggregator_result(res, result):
    if len(res['queries'][0]['results'][0]['tags']) and len(res['queries'][0]['results'][0]['values']):
        assert result == [res['queries'][0]['results'][0]['values'][0][1]]
    else:
        assert result == []


def test_history_result(monkeypatch, fx_result):
    wrapper_kwargs, kwargs, res = fx_result

    post, get_request = mock_all(monkeypatch, res)

    cli = HistoryWrapper(url=URL, **wrapper_kwargs)

    result = cli.result(**kwargs)
    assert result == res

    post.assert_called_with(get_final_url(), json=get_request.return_value)

    assert_get_request(get_request, kwargs, wrapper_kwargs)


def test_history_get_one(monkeypatch, fx_result):
    wrapper_kwargs, kwargs, res = fx_result

    post, get_request = mock_all(monkeypatch, res)

    cli = HistoryWrapper(url=URL, **wrapper_kwargs)

    result = cli.get_one(**kwargs)
    assert result == res['queries'][0]['results'][0]['values']

    post.assert_called_with(get_final_url(), json=get_request.return_value)

    assert_get_request(get_request, kwargs, wrapper_kwargs)


def test_history_get_aggregated(monkeypatch, fx_result):
    wrapper_kwargs, kwargs, res = fx_result

    post, get_request = mock_all(monkeypatch, res)

    cli = HistoryWrapper(url=URL, **wrapper_kwargs)

    aggregator = 'sum'
    result = cli.get_aggregated(FILTER_KEY, aggregator, **kwargs)

    assert_aggregator_result(res, result)

    post.assert_called_with(get_final_url(), json=get_request.return_value)

    assert_get_request(get_request, kwargs, wrapper_kwargs, aggregator)


def test_history_get_avg(monkeypatch, fx_result):
    wrapper_kwargs, kwargs, res = fx_result

    post, get_request = mock_all(monkeypatch, res)

    cli = HistoryWrapper(url=URL, **wrapper_kwargs)

    aggregator = 'avg'
    result = cli.get_avg(FILTER_KEY, **kwargs)

    assert_aggregator_result(res, result)

    post.assert_called_with(get_final_url(), json=get_request.return_value)

    assert_get_request(get_request, kwargs, wrapper_kwargs, aggregator)


def test_history_get_std_dev(monkeypatch, fx_result):
    wrapper_kwargs, kwargs, res = fx_result

    post, get_request = mock_all(monkeypatch, res)

    cli = HistoryWrapper(url=URL, **wrapper_kwargs)

    aggregator = 'dev'
    result = cli.get_std_dev(FILTER_KEY, **kwargs)

    assert_aggregator_result(res, result)

    post.assert_called_with(get_final_url(), json=get_request.return_value)

    assert_get_request(get_request, kwargs, wrapper_kwargs, aggregator)


def test_history_result_oauth2(monkeypatch):
    token = 123
    get = MagicMock()
    get.return_value = token
    monkeypatch.setattr('tokens.get', get)

    cli = HistoryWrapper(url=URL, oauth2=True)

    assert 'Bearer {}'.format(token) == cli._HistoryWrapper__session.headers['Authorization']


def test_history_result_error():
    with pytest.raises(ConfigurationError):
        HistoryWrapper()
