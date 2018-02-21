import json

import pytest
from mock import MagicMock

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.notifications.http import NotifyHttp, NotificationError


URL = 'http://notify-service'


def get_headers(headers=None):
    h = {'User-Agent': get_user_agent()}

    if headers:
        h.update(headers)

    return h


def test_http_notification(monkeypatch):
    post = MagicMock()

    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 1}}

    NotifyHttp._config = {'notifications.http.default.url': URL}
    r = NotifyHttp.notify(alert)

    data = {'alert': alert, 'body': None}

    assert r == 0

    post.assert_called_with(URL, data=json.dumps(data), params=None, headers=get_headers(), timeout=5)


def test_http_notification_oauth2(monkeypatch):
    post = MagicMock()
    get = MagicMock()
    get.return_value = 123

    monkeypatch.setattr('requests.post', post)
    monkeypatch.setattr('tokens.get', get)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 1}}

    NotifyHttp._config = {'notifications.http.default.url': URL}
    r = NotifyHttp.notify(alert, oauth2=True)

    data = {'alert': alert, 'body': None}

    headers = {'Authorization': 'Bearer 123'}

    assert r == 0

    post.assert_called_with(URL, data=json.dumps(data), params=None, headers=get_headers(headers), timeout=5)


@pytest.mark.parametrize('urls,result', [
    ('http://some-other-service, https://some-3rd-service', NotificationError),
    ('http://some-other-service, http://notify-service', None),
])
def test_http_notification_check_allowed(monkeypatch, urls, result):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 1}}

    NotifyHttp._config = {'notifications.http.whitelist.urls': urls}

    if result is None:
        r = NotifyHttp.notify(alert, url=URL)

        data = {'alert': alert, 'body': None}

        assert r == 0

        post.assert_called_with(URL, data=json.dumps(data), params=None, headers=get_headers(), timeout=5)
    else:
        with pytest.raises(NotificationError):
            NotifyHttp.notify(alert, url=URL)


def test_http_notification_allow_all(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 1}}

    NotifyHttp._config = {
        'notifications.http.whitelist.urls': 'http://some-other-service, https://some-3rd-service',
        'notifications.http.allow.all': True,
    }

    r = NotifyHttp.notify(alert, url=URL)

    data = {'alert': alert, 'body': None}

    assert r == 0
    post.assert_called_with(URL, data=json.dumps(data), params=None, headers=get_headers(), timeout=5)


def test_http_notification_url_error(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 1}}

    NotifyHttp._config = {}
    with pytest.raises(NotificationError):
        NotifyHttp.notify(alert, url='some-service/notify')


@pytest.mark.parametrize('include_alert', [True, False])
def test_http_notification_args(monkeypatch, include_alert):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 1}}
    body = {'zmon': True}
    params = {'token': 123}
    headers = {'X-CSRFTOKEN': '1234'}

    NotifyHttp._config = {'notifications.http.default.url': URL}

    r = NotifyHttp.notify(alert, include_alert=include_alert, body=body, params=params, headers=headers, repeat=4)

    data = {'alert': alert, 'body': body}

    if not include_alert:
        data = body

    assert r == 4

    post.assert_called_with(URL, data=json.dumps(data), params=params, headers=get_headers(headers), timeout=5)
