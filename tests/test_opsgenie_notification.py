import json

import pytest

from datetime import datetime

from mock import MagicMock

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder

from zmon_worker_monitor.zmon_worker.notifications.opsgenie import NotifyOpsgenie, NotificationError, get_user_agent


URL_CREATE = 'https://api.opsgenie.com/v1/json/alert'
URL_CLOSE = 'https://api.opsgenie.com/v1/json/alert/close'
API_KEY = '123'

MESSAGE = 'ZMON ALERT'

HEADERS = {'User-Agent': get_user_agent(), 'Content-type': 'application/json'}


@pytest.mark.parametrize('is_alert', (True, False))
def test_opsgenie_notification(monkeypatch, is_alert):
    post = MagicMock()

    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': is_alert, 'alert_def': {'id': 123}, 'entity': {'id': 'e-1'}}

    NotifyOpsgenie._config = {'notifications.opsgenie.apikey': API_KEY}

    r = NotifyOpsgenie.notify(alert, message=MESSAGE, include_alert=False)

    data = {
        'apiKey': API_KEY,
        'alias': 'ZMON-123',
        'source': 'ZMON',
        'note': '',
    }

    if is_alert:
        data['message'] = MESSAGE
        data['details'] = {}
        data['entity'] = 'e-1'

    assert r == 0

    URL = URL_CREATE if is_alert else URL_CLOSE

    post.assert_called_with(URL, data=json.dumps(data, cls=JsonDataEncoder), headers=HEADERS, timeout=5)


def test_opsgenie_notification_error_api_key(monkeypatch):
    NotifyOpsgenie._config = {}

    with pytest.raises(NotificationError):
        NotifyOpsgenie.notify({}, message=MESSAGE)


def test_opsgenie_notification_exception(monkeypatch):
    post = MagicMock()
    post.side_effect = Exception
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 123}, 'entity': {'id': 'e-1'}}

    NotifyOpsgenie._config = {'notifications.opsgenie.apikey': API_KEY}

    r = NotifyOpsgenie.notify(alert, message=MESSAGE, per_entity=True)

    assert r == 0


def test_opsgenie_notification_per_entity(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {
        'changed': True, 'is_alert': True, 'alert_def': {'id': 123}, 'entity': {'id': 'e-1'}, 'time': datetime.now()
    }

    NotifyOpsgenie._config = {
        'notifications.opsgenie.apikey': API_KEY,
        'zmon.host': 'https://zmon.example.org/'
    }

    r = NotifyOpsgenie.notify(alert, message=MESSAGE, per_entity=True)

    data = {
        'apiKey': API_KEY,
        'alias': 'ZMON-123-e-1',
        'message': MESSAGE,
        'source': 'ZMON',
        'note': 'https://zmon.example.org/#/alert-details/123',
        'entity': 'e-1',
        'details': alert,
    }

    assert r == 0

    post.assert_called_with(URL_CREATE, data=json.dumps(data, cls=JsonDataEncoder), headers=HEADERS, timeout=5)
