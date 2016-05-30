import os

import requests
import pytest

from mock import MagicMock

from zmon_worker_monitor.zmon_worker.errors import HttpError
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from zmon_worker_monitor.builtins.plugins.elasticsearch import ElasticsearchWrapper
from zmon_worker_monitor.builtins.plugins.elasticsearch import DEFAULT_SIZE, MAX_SIZE, MAX_INDICES


def get_full_url(url, indices=None, health=False):
    if health:
        return os.path.join(url, '_cluster', 'health')

    indices_str = ''
    if indices:
        indices_str = ','.join(indices)

    return os.path.join(url, indices_str, '_search')


def resp_mock(failure=False):
    resp = MagicMock()
    resp.ok = True if not failure else False
    json_res = {'hits': []} if not failure else {'error': {}, 'status': 400}
    resp.json.return_value = json_res

    return resp


def requests_mock(resp, failure=None):
    req = MagicMock()

    if failure is not None:
        req.side_effect = failure
    else:
        req.return_value = resp

    return req


def test_elasticsearch_search(monkeypatch):
    resp = resp_mock()
    get = requests_mock(resp)
    monkeypatch.setattr('requests.get', get)

    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    result = es.search()

    assert result == resp.json.return_value

    get.assert_called_with(get_full_url(url),
                           headers={'User-Agent': get_user_agent()},
                           params={'q': '', 'size': DEFAULT_SIZE, '_source': 'true'},
                           timeout=10)


def test_elasticsearch_search_multiple_indices(monkeypatch):
    resp = resp_mock()
    get = requests_mock(resp)
    monkeypatch.setattr('requests.get', get)

    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    indices = ['logstash-2016-*', 'logstash-2015-*']
    result = es.search(indices=indices)

    assert result == resp.json.return_value

    get.assert_called_with(get_full_url(url, indices=indices),
                           headers={'User-Agent': get_user_agent()},
                           params={'q': '', 'size': DEFAULT_SIZE, '_source': 'true'},
                           timeout=10)


def test_elasticsearch_search_body(monkeypatch):
    resp = resp_mock()
    post = requests_mock(resp)
    monkeypatch.setattr('requests.post', post)

    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    body = {'query': {'query_string': {'query': ''}}}
    result = es.search(body=body)

    assert result == resp.json.return_value

    body['size'] = DEFAULT_SIZE

    post.assert_called_with(get_full_url(url),
                            json=body,
                            headers={'User-Agent': get_user_agent()},
                            timeout=10)


def test_elasticsearch_search_multiple_indices_body(monkeypatch):
    resp = resp_mock()
    post = requests_mock(resp)
    monkeypatch.setattr('requests.post', post)

    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    body = {'query': {'query_string': {'query': ''}}}

    indices = ['logstash-2016-*', 'logstash-2015-*']
    result = es.search(indices=indices, body=body)

    assert result == resp.json.return_value

    body['size'] = DEFAULT_SIZE

    post.assert_called_with(get_full_url(url, indices=indices),
                            json=body,
                            headers={'User-Agent': get_user_agent()},
                            timeout=10)


def test_elasticsearch_search_no_source_with_size(monkeypatch):
    resp = resp_mock()
    get = requests_mock(resp)
    monkeypatch.setattr('requests.get', get)

    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    result = es.search(source=False, size=100)

    assert result == resp.json.return_value

    get.assert_called_with(get_full_url(url),
                           headers={'User-Agent': get_user_agent()},
                           params={'q': '', 'size': 100, '_source': 'false'},
                           timeout=10)


def test_elasticsearch_search_no_source_body_with_size(monkeypatch):
    resp = resp_mock()
    post = requests_mock(resp)
    monkeypatch.setattr('requests.post', post)

    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    body = {'query': {'query_string': {'query': ''}}}

    indices = ['logstash-2016-*', 'logstash-2015-*']
    result = es.search(indices=indices, body=body, source=False, size=100)

    assert result == resp.json.return_value

    body['size'] = 100
    body['_source'] = False

    post.assert_called_with(get_full_url(url, indices=indices),
                            json=body,
                            headers={'User-Agent': get_user_agent()},
                            timeout=10)


def test_elasticsearch_search_oauth2(monkeypatch):
    resp = resp_mock()
    get = requests_mock(resp)
    monkeypatch.setattr('requests.get', get)

    # mock tokens
    token = 'TOKEN-123'
    monkeypatch.setattr('tokens.get', lambda x: token)

    url = 'http://es/'
    es = ElasticsearchWrapper(url, oauth2=True)

    result = es.search()

    assert result == resp.json.return_value

    get.assert_called_with(get_full_url(url),
                           headers={'User-Agent': get_user_agent(), 'Authorization': 'Bearer {}'.format(token)},
                           params={'q': '', 'size': DEFAULT_SIZE, '_source': 'true'},
                           timeout=10)


def test_elasticsearch_health(monkeypatch):
    resp = resp_mock()
    get = requests_mock(resp)
    monkeypatch.setattr('requests.get', get)

    # mock tokens
    token = 'TOKEN-123'
    monkeypatch.setattr('tokens.get', lambda x: token)

    url = 'http://es/'
    es = ElasticsearchWrapper(url, oauth2=True)

    result = es.health()

    assert result == resp.json.return_value

    get.assert_called_with(get_full_url(url, health=True),
                           headers={'User-Agent': get_user_agent(), 'Authorization': 'Bearer {}'.format(token)},
                           params=None,
                           timeout=10)


def test_elasticsearch_invalid_size(monkeypatch):
    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    with pytest.raises(Exception):
        es.search(size=MAX_SIZE + 1)

    with pytest.raises(Exception):
        es.search(size=-1)


def test_elasticsearch_invalid_indices(monkeypatch):
    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    with pytest.raises(Exception):
        es.search(indices={'index-1': 'not a list!'})

    with pytest.raises(Exception):
        es.search(indices=['index-*'] * MAX_INDICES + 1)


def test_elasticsearch_search_error(monkeypatch):
    resp = resp_mock()

    get = requests_mock(resp, failure=requests.Timeout)
    monkeypatch.setattr('requests.get', get)

    url = 'http://es/'
    es = ElasticsearchWrapper(url)

    with pytest.raises(HttpError):
        es.search()

    get = requests_mock(resp, failure=requests.ConnectionError)
    monkeypatch.setattr('requests.get', get)

    with pytest.raises(HttpError):
        es.search()

    # Unhandled exception
    get = requests_mock(resp, failure=Exception)
    monkeypatch.setattr('requests.get', get)

    with pytest.raises(Exception) as ex:
        es.search()

        assert ex is not HttpError


def test_elasticsearch_no_url():
    with pytest.raises(RuntimeError):
        ElasticsearchWrapper()
