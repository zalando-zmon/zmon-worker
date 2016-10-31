import pytest
from mock import MagicMock

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.notifications.slack import NotifySlack, NotificationError


URL = 'http://slack-webhook'

HEADERS = {
    'User-agent': get_user_agent(),
    'Content-type': 'application/json',
}


def test_slack_notification(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 123, 'name': 'alert'}, 'entity': {'id': 'e-1'}}

    NotifySlack._config = {'notifications.slack.webhook': URL}

    r = NotifySlack.notify(alert, message='ALERT')

    data = {
        'username': 'ZMON',
        'channel': '#general',
        'text': 'ALERT',
        'icon_emoji': ':bar_chart:',
    }

    assert r == 0

    post.assert_called_with(URL, json=data, headers=HEADERS, timeout=5)


def test_slack_notification_url_error(monkeypatch):
    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 123, 'name': 'alert'}, 'entity': {'id': 'e-1'}}

    NotifySlack._config = {}
    with pytest.raises(NotificationError):
        NotifySlack.notify(alert, message='ALERT')


def test_slack_notification_error(monkeypatch):
    post = MagicMock()
    post.side_effect = Exception
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 123, 'name': 'alert'}, 'entity': {'id': 'e-1'}}

    NotifySlack._config = {'notifications.slack.webhook': URL}

    r = NotifySlack.notify(alert, message='ALERT')
    assert r == 0
