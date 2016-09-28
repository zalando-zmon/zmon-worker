import json

import pytest

from datetime import datetime

from mock import MagicMock

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder

from zmon_worker_monitor.zmon_worker.notifications.pagerduty import NotifyPagerduty, NotificationError, get_user_agent


URL = 'https://events.pagerduty.com/generic/2010-04-15/create_event.json'
SERVICE_KEY = '123'

MESSAGE = 'ZMON ALERT'

HEADERS = {'User-Agent': get_user_agent(), 'Content-type': 'application/json'}


@pytest.mark.parametrize('is_alert', (True, False))
def test_pagerduty_notification(monkeypatch, is_alert):
    post = MagicMock()

    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': is_alert, 'alert_def': {'id': 123}, 'entity': {'id': 'e-1'}}

    NotifyPagerduty._config = {'notifications.pagerduty.servicekey': SERVICE_KEY}

    r = NotifyPagerduty.notify(alert, message=MESSAGE, include_alert=False)

    data = {
        'service_key': SERVICE_KEY,
        'event_type': 'trigger' if is_alert else 'resolve',
        'incident_key': 'ZMON-123',
        'description': MESSAGE,
        'client': 'ZMON',
        'client_url': '',
        'details': '',
    }

    assert r == 0

    post.assert_called_with(URL, json=data, headers=HEADERS, timeout=5)


def test_pagerduty_notification_error_service_key(monkeypatch):
    NotifyPagerduty._config = {}

    with pytest.raises(NotificationError):
        NotifyPagerduty.notify({}, message=MESSAGE)


def test_pagerduty_notification_exception(monkeypatch):
    post = MagicMock()
    post.side_effect = Exception
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 123}, 'entity': {'id': 'e-1'}}

    NotifyPagerduty._config = {'notifications.pagerduty.servicekey': SERVICE_KEY}

    r = NotifyPagerduty.notify(alert, message=MESSAGE, per_entity=True)

    assert r == 0


def test_pagerduty_notification_per_entity(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {
        'changed': True, 'is_alert': True, 'alert_def': {'id': 123}, 'entity': {'id': 'e-1'}, 'time': datetime.now()
    }

    NotifyPagerduty._config = {
        'notifications.pagerduty.servicekey': SERVICE_KEY,
        'zmon.host': 'https://zmon.example.org/'
    }

    r = NotifyPagerduty.notify(alert, message=MESSAGE, per_entity=True)

    data = {
        'service_key': SERVICE_KEY,
        'event_type': 'trigger',
        'incident_key': 'ZMON-123-e-1',
        'description': MESSAGE,
        'client': 'ZMON',
        'client_url': 'https://zmon.example.org/#/alert-details/123',
        'details': json.dumps(alert, cls=JsonDataEncoder),
    }

    assert r == 0

    post.assert_called_with(URL, json=data, headers=HEADERS, timeout=5)
