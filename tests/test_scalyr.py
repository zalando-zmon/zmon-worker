import pytest
import requests

from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.scalyr import ScalyrWrapper, ConfigurationError


@pytest.fixture(params=[
    (
        {'query': 'filter-query'},
        {
            'results': [{
                'values': [5],
            }],
            'status': 'success'
        },
        5
    ),
    (
        {'query': 'filter-query', 'minutes': 10},
        {
            'results': [{
                'values': [5]
            }],
            'status': 'success'
        },
        5
    ),
    (
        {'query': 'filter-query'},
        {'status': 'failed', 'message': 'error'},
        {'status': 'failed', 'message': 'error'},
    )
])
def fx_count(request):
    return request.param


@pytest.fixture(params=[
    (
        {'query': 'query-sum', 'function': 'sum'},
        {'values': [5, 4, 3, 2]},
        5
    ),
    (
        {'query': 'query-sum', 'function': 'sum', 'minutes': 10},
        {'values': [100, 4, 3, 2]},
        100
    ),
    (
        {'query': 'query-sum', 'function': 'sum'},
        {'sum': 5},
        {'sum': 5},
    )
])
def fx_function(request):
    return request.param


@pytest.fixture(params=[
    (
        {'filter': 'facet-filter', 'field': 'field1'},
        {'values': [5, 4, 3, 2]},
    ),
    (
        {'filter': 'facet-filter', 'field': 'field1', 'minutes': 10},
        {'values': [100, 4, 3, 2]},
    ),
    (
        {'filter': 'facet-filter', 'field': 'field1', 'max_count': 100, 'prio': 'high'},
        {'sum': 5},
    )
])
def fx_facets(request):
    return request.param


@pytest.fixture(params=[
    (
        {'filter': 'filter-query'},
        {
            'results': [{
                'values': [[5]]
            }],
            'status': 'success'
        },
        [5]
    ),
    (
        {'filter': 'filter-query', 'function': 'sum', 'minutes': 10, 'buckets': 2},
        {
            'results': [{
                'values': [5, 4, 3, 2, 1]
            }],
            'status': 'success'
        },
        [25, 20, 15, 10, 5]
    ),
    (
        {'filter': 'filter-query', 'function': 'sum', 'prio': 'high'},
        {
            'results': [{
                'values': []
            }],
            'status': 'success'
        },
        [],
    ),
    (
        {'filter': 'filter-query', 'function': 'sum', 'prio': 'high'},
        {'status': 'failed', 'message': 'error'},
        {'status': 'failed', 'message': 'error'},
    )
])
def fx_timeseries(request):
    return request.param


def get_query(query_type, func, key, **kwargs):
    q = {
        'token': key,
        'queryType': query_type,
        'filter': kwargs.get('query', '') or kwargs.get('filter', ''),
        'function': func,
        'startTime': str(kwargs.get('minutes', '5')) + 'm',
        'priority': kwargs.get('prio', 'low'),
        'buckets': kwargs.get('buckets', 1)
    }

    if 'field' in kwargs:
        q['field'] = kwargs['field']

    return q


def test_scalyr_eu_region():
    read_key = '123'
    region = 'eu'
    numeric_url = 'https://eu.scalyr.com/api/numericQuery'
    timeseries_url = 'https://eu.scalyr.com/api/timeseriesQuery'
    facet_url = 'https://eu.scalyr.com/api/facetQuery'

    scalyr = ScalyrWrapper(read_key, region)

    assert numeric_url == scalyr._ScalyrWrapper__numeric_url
    assert timeseries_url == scalyr._ScalyrWrapper__timeseries_url
    assert facet_url == scalyr._ScalyrWrapper__facet_url


def test_scalyr_count(monkeypatch, fx_count):
    kwargs, res, exp = fx_count

    read_key = '123'

    post = MagicMock()
    post.return_value.json.return_value = res

    monkeypatch.setattr('requests.post', post)

    scalyr = ScalyrWrapper(read_key)
    count = scalyr.count(**kwargs)

    assert count == exp

    query = get_query('facet', kwargs.get('function', 'count'), read_key, **kwargs)

    query.pop('queryType')

    final_q = {
        'token': query.pop('token'),
        'queries': [query]
    }

    post.assert_called_with(
        scalyr._ScalyrWrapper__timeseries_url, json=final_q, headers={'Content-Type': 'application/json'})


def test_scalyr_function(monkeypatch, fx_function):
    kwargs, res, exp = fx_function

    read_key = '123'

    post = MagicMock()
    post.return_value.json.return_value = res

    monkeypatch.setattr('requests.post', post)

    scalyr = ScalyrWrapper(read_key)
    count = scalyr.function(**kwargs)

    assert count == exp

    query = get_query('numeric', kwargs['function'], read_key, **kwargs)

    post.assert_called_with(
        scalyr._ScalyrWrapper__numeric_url, json=query, headers={'Content-Type': 'application/json'})


def test_scalyr_facets(monkeypatch, fx_facets):
    kwargs, res = fx_facets

    read_key = '123'

    post = MagicMock()
    post.return_value.json.return_value = res

    monkeypatch.setattr('requests.post', post)

    scalyr = ScalyrWrapper(read_key)
    result = scalyr.facets(**kwargs)

    assert result == res

    if 'minutes' not in kwargs:
        kwargs['minutes'] = 30
    query = get_query(
        'facet', None, read_key, **kwargs)

    query.pop('function', None)
    query.pop('buckets', None)
    query['maxCount'] = kwargs.get('max_count', 5)

    post.assert_called_with(
        scalyr._ScalyrWrapper__facet_url, json=query, headers={'Content-Type': 'application/json'})


def test_scalyr_timeseries(monkeypatch, fx_timeseries):
    kwargs, res, exp = fx_timeseries

    read_key = '123'

    post = MagicMock()
    post.return_value.json.return_value = res

    monkeypatch.setattr('requests.post', post)

    scalyr = ScalyrWrapper(read_key)
    result = scalyr.timeseries(**kwargs)

    assert result == exp

    if 'minutes' not in kwargs:
        kwargs['minutes'] = 30
    query = get_query('facet', kwargs.get('function', 'count'), read_key, **kwargs)

    query.pop('queryType')

    final_q = {
        'token': query.pop('token'),
        'queries': [query]
    }

    post.assert_called_with(
        scalyr._ScalyrWrapper__timeseries_url, json=final_q, headers={'Content-Type': 'application/json'})


def test_scalyr_config_error(monkeypatch):
    with pytest.raises(ConfigurationError):
        ScalyrWrapper('')


@pytest.mark.parametrize(
    'method', [('count', ['q']), ('function', ['f', 'q']), ('timeseries', ['f']), ('facets', ['f', 'f'])])
def test_scalyr_http_error(monkeypatch, method):
    post = MagicMock()
    post.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError

    monkeypatch.setattr('requests.post', post)

    scalyr = ScalyrWrapper('123')

    m, args = method
    f = getattr(scalyr, m)
    with pytest.raises(requests.exceptions.HTTPError):
        f(*args)
