import os
import json

import pytest
import requests

from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.entities_wrapper import EntitiesWrapper, HttpError, CheckError


@pytest.mark.parametrize('kwargs', [{'infrastructure_account': 'aws:acc1', 'type': 'instance'}, {}])
def test_entities_search_local(monkeypatch, kwargs):
    result = [1, 2, 3]

    get = MagicMock()
    get.return_value.json.return_value = result
    get.return_value.ok = True

    monkeypatch.setattr('requests.Session.get', get)

    url = 'https://service-url'
    ia = 'aws:acc-default'

    e = EntitiesWrapper(service_url=url, infrastructure_account=ia)

    res = e.search_local(**kwargs)

    assert res == result

    entities_url = os.path.join(url, 'api/v1/entities')

    if 'infrastructure_account' not in kwargs:
        kwargs['infrastructure_account'] = ia

    q = {'query': json.dumps(kwargs)}

    get.assert_called_with(entities_url, params=q)


@pytest.mark.parametrize('kwargs', [{'infrastructure_account': 'aws:acc1', 'type': 'instance'}, {}])
def test_entities_search_all(monkeypatch, kwargs):
    result = [1, 2, 3]

    get = MagicMock()
    get.return_value.json.return_value = result
    get.return_value.ok = True

    monkeypatch.setattr('requests.Session.get', get)

    url = 'https://service-url'
    ia = 'aws:acc-default'

    e = EntitiesWrapper(service_url=url, infrastructure_account=ia)

    res = e.search_all(**kwargs)

    assert res == result

    entities_url = os.path.join(url, 'api/v1/entities')

    q = {'query': json.dumps(kwargs)}

    get.assert_called_with(entities_url, params=q)


@pytest.mark.parametrize('kwargs', [{'infrastructure_account': 'aws:acc1', 'type': 'instance'}, {}])
def test_entities_alert_coverage(monkeypatch, kwargs):
    result = [1, 2, 3]

    post = MagicMock()
    post.return_value.json.return_value = result
    post.return_value.ok = True

    monkeypatch.setattr('requests.Session.post', post)

    url = 'https://service-url'
    ia = 'aws:acc-default'

    e = EntitiesWrapper(service_url=url, infrastructure_account=ia)

    res = e.alert_coverage(**kwargs)

    assert res == result

    entities_url = os.path.join(url, 'api/v1/status/alert-coverage')

    if 'infrastructure_account' not in kwargs:
        kwargs['infrastructure_account'] = ia

    q = [kwargs]

    post.assert_called_with(entities_url, json=q)


@pytest.mark.parametrize('exc', [requests.ConnectionError, requests.Timeout, RuntimeError])
def test_entities_exception(monkeypatch, exc):
    get = MagicMock()
    get.side_effect = exc

    monkeypatch.setattr('requests.Session.get', get)

    url = 'https://service-url'
    ia = 'aws:acc-default'

    e = EntitiesWrapper(service_url=url, infrastructure_account=ia)

    expected = HttpError if issubclass(exc, requests.RequestException) else exc

    with pytest.raises(expected):
        e.search_all(**{})


def test_entities_error(monkeypatch):
    err = 'ERROR'

    get = MagicMock()
    get.return_value.ok = False
    get.return_value.text = err
    get.return_value.status_code = 404

    monkeypatch.setattr('requests.Session.get', get)

    url = 'https://service-url'
    ia = 'aws:acc-default'

    e = EntitiesWrapper(service_url=url, infrastructure_account=ia)

    with pytest.raises(CheckError) as ex:
        e.search_local(**{})

        assert err in str(ex)
