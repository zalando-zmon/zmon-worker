import json

import pytest

from datetime import datetime

from mock import MagicMock

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder

from zmon_worker_monitor.zmon_worker.notifications.pagerduty import NotifyPagerduty, NotificationError, get_user_agent


URL = 'https://events.pagerduty.com/v2/enqueue'
SERVICE_KEY = '123'

MESSAGE = 'ZMON ALERT'

HEADERS = {'User-Agent': get_user_agent(), 'Content-type': 'application/json'}


@pytest.mark.parametrize('is_alert', (True, False))
def test_pagerduty_notification(monkeypatch, is_alert):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {
        'alert_changed': True, 'is_alert': is_alert, 'alert_def': {'id': 123, 'priority': 1}, 'entity': {'id': 'e-1'},
        'alert_evaluation_ts': 1234, 'worker': 'worker-1',
    }

    NotifyPagerduty._config = {'notifications.pagerduty.servicekey': SERVICE_KEY}

    r = NotifyPagerduty.notify(alert, message=MESSAGE, include_alert=False)

    data = {
        'routing_key': SERVICE_KEY,
        'event_action': 'trigger' if is_alert else 'resolve',
        'dedup_key': 'ZMON-123',
        'client': 'ZMON',
        'client_url': '',
        'payload': {
            'summary': MESSAGE,
            'source': 'worker-1',
            'severity': 'critical',
            'component': alert['entity']['id'],
            'custom_details': {'alert_evaluation_ts': 1234},
            'class': '',
            'group': '',
        },
    }

    assert r == 0

    post.assert_called_with(URL, data=json.dumps(data, cls=JsonDataEncoder), headers=HEADERS, timeout=5)


def test_pagerduty_notification_no_change(monkeypatch):
    alert = {
        'is_alert': True, 'alert_changed': False, 'alert_def': {'id': 123, 'priority': 1},
        'entity': {'id': 'e-1'}, 'worker': 'worker-1',
    }

    NotifyPagerduty._config = {'notifications.pagerduty.servicekey': SERVICE_KEY}

    r = NotifyPagerduty.notify(alert, message=MESSAGE, include_alert=False)

    assert r == 0


def test_pagerduty_notification_error_service_key(monkeypatch):
    NotifyPagerduty._config = {}

    alert = {
        'is_alert': True, 'alert_changed': False, 'alert_def': {'id': 123, 'priority': 1},
        'entity': {'id': 'e-1'}, 'worker': 'worker-1',
    }

    with pytest.raises(NotificationError):
        NotifyPagerduty.notify(alert, message=MESSAGE)


def test_pagerduty_notification_exception(monkeypatch):
    post = MagicMock()
    post.side_effect = Exception
    monkeypatch.setattr('requests.post', post)

    alert = {'alert_changed': True, 'is_alert': True, 'alert_def': {'id': 123, 'priority': 3}, 'entity': {'id': 'e-1'}}

    NotifyPagerduty._config = {'notifications.pagerduty.servicekey': SERVICE_KEY}

    r = NotifyPagerduty.notify(alert, message=MESSAGE, per_entity=True)

    assert r == 0


def test_pagerduty_notification_per_entity(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {
        'alert_changed': True, 'is_alert': True, 'alert_def': {'id': 123, 'priority': 3}, 'entity': {'id': 'e-1'},
        'alert_evaluation_ts': 1234, 'time': datetime.now(), 'worker': 'worker-1',
    }

    NotifyPagerduty._config = {
        'notifications.pagerduty.servicekey': SERVICE_KEY,
        'zmon.host': 'https://zmon.example.org/'
    }

    r = NotifyPagerduty.notify(alert, message=MESSAGE, per_entity=True, alert_class='Health', alert_group='production')

    data = {
        'routing_key': SERVICE_KEY,
        'event_action': 'trigger',
        'dedup_key': 'ZMON-123-e-1',
        'client': 'ZMON',
        'client_url': 'https://zmon.example.org/#/alert-details/123',
        'payload': {
            'summary': MESSAGE,
            'source': 'worker-1',
            'severity': 'error',
            'component': alert['entity']['id'],
            'custom_details': alert,
            'class': 'Health',
            'group': 'production',
        },
    }

    assert r == 0

    post.assert_called_with(URL, data=json.dumps(data, cls=JsonDataEncoder), headers=HEADERS, timeout=5)
