import json

import pytest
import requests

from datetime import datetime

from mock import MagicMock

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder

from zmon_worker_monitor.zmon_worker.notifications.opsgenie import NotifyOpsgenie, NotificationError, get_user_agent

URL_CREATE = 'https://api.opsgenie.com/v2/alerts'
URL_CLOSE = 'https://api.opsgenie.com/v2/alerts/{}/close'
API_KEY = '123'

MESSAGE = 'ZMON ALERT'

HEADERS = {
    'User-Agent': get_user_agent(),
    'Content-type': 'application/json',
    'Authorization': 'GenieKey {}'.format(API_KEY),
}


@pytest.mark.parametrize('is_alert,priority,override_description,set_custom_fileds',
                         ((True, None, None, None),
                          (True, 'P4', None, None),
                          (False, None, None, None),
                          (True, None, "override description", None),
                          (True, None, None, {'custom_field': 'values'}))
                         )
def test_opsgenie_notification(monkeypatch, is_alert, priority, override_description, set_custom_fileds):
    post = MagicMock()

    monkeypatch.setattr('requests.post', post)

    alert = {
        'alert_changed': True, 'changed': True, 'is_alert': is_alert, 'entity': {'id': 'e-1'}, 'worker': 'worker-1',
        'alert_evaluation_ts': 1234,
        'alert_def': {
            'name': 'Alert',
            'team': 'zmon',
            'responsible_team': 'zmon',
            'id': 123,
            'priority': 1,
        }
    }

    NotifyOpsgenie._config = {'notifications.opsgenie.apikey': API_KEY}

    kwargs = {}
    if priority:
        kwargs['priority'] = priority
    else:
        priority = 'P1'

    if override_description:
        r = NotifyOpsgenie.notify(
            alert,
            message=MESSAGE,
            include_alert=False,
            teams=['team-1', 'team-2'],
            description=override_description,
            custom_fields=set_custom_fileds,
            **kwargs
        )
    else:
        r = NotifyOpsgenie.notify(
            alert,
            message=MESSAGE,
            include_alert=False,
            teams=['team-1', 'team-2'],
            custom_fields=set_custom_fileds,
            **kwargs
        )

    params = {}

    if is_alert:
        details = {'alert_evaluation_ts': 1234}

        if set_custom_fileds:
            details.update(set_custom_fileds)

        data = {
            'alias': 'ZMON-123',
            'message': MESSAGE,
            'description': '',
            'entity': 'e-1',
            'priority': priority,
            'tags': [123],
            'teams': [{'name': 'team-1'}, {'name': 'team-2'}],
            'source': 'worker-1',
            'note': '',
            'details': details,
        }

        if override_description:
            data['description'] = override_description

    else:
        data = {
            'user': 'ZMON',
            'source': 'worker-1',
            'note': '',

        }

        params = {'identifierType': 'alias'}

    assert r == 0

    URL = URL_CREATE if is_alert else URL_CLOSE.format('ZMON-123')

    post.assert_called_with(URL, data=json.dumps(data, cls=JsonDataEncoder, sort_keys=True), headers=HEADERS, timeout=5,
                            params=params)


@pytest.mark.parametrize('include_captures, is_alert,priority,override_description',
                         ((True, True, None, None),
                          (True, True, 'P4', None),
                          (False, False, None, None),
                          (False, True, None, "override description"))
                         )
def test_opsgenie_notification_captures(monkeypatch, include_captures, is_alert, priority, override_description):
    post = MagicMock()

    monkeypatch.setattr('requests.post', post)

    alert = {
        'alert_changed': True, 'changed': True, 'is_alert': is_alert, 'entity': {'id': 'e-1'}, 'worker': 'worker-1',
        'alert_evaluation_ts': 1234,
        'captures': {'foo': 'bar'},
        'alert_def': {
            'name': 'Alert',
            'team': 'zmon',
            'responsible_team': 'zmon',
            'id': 123,
            'priority': 1,
        }
    }

    NotifyOpsgenie._config = {'notifications.opsgenie.apikey': API_KEY}

    kwargs = {}
    if priority:
        kwargs['priority'] = priority
    else:
        priority = 'P1'

    if override_description:
        r = NotifyOpsgenie.notify(
            alert,
            message=MESSAGE,
            include_alert=False,
            include_captures=include_captures,
            teams=['team-1', 'team-2'],
            description=override_description,
            **kwargs
        )
    else:
        r = NotifyOpsgenie.notify(
            alert,
            message=MESSAGE,
            include_alert=False,
            include_captures=include_captures,
            teams=['team-1', 'team-2'],
            **kwargs
        )

    params = {}

    if include_captures:
        details = {'alert_evaluation_ts': 1234, 'foo': 'bar'}
    else:
        details = {'alert_evaluation_ts': 1234}

    if is_alert:
        data = {
            'alias': 'ZMON-123',
            'message': MESSAGE,
            'description': '',
            'entity': 'e-1',
            'priority': priority,
            'tags': [123],
            'teams': [{'name': 'team-1'}, {'name': 'team-2'}],
            'source': 'worker-1',
            'note': '',
            'details': details,
        }

        if override_description:
            data['description'] = override_description

    else:
        data = {
            'user': 'ZMON',
            'source': 'worker-1',
            'note': '',

        }

        params = {'identifierType': 'alias'}

    assert r == 0

    URL = URL_CREATE if is_alert else URL_CLOSE.format('ZMON-123')

    post.assert_called_with(URL, data=json.dumps(data, cls=JsonDataEncoder, sort_keys=True), headers=HEADERS, timeout=5,
                            params=params)


def test_opsgenie_notification_per_entity(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {
        'changed': True, 'is_alert': True, 'entity': {'id': 'e-1', 'application': 'app_id'}, 'worker': 'worker-1',
        'time': datetime.now(),
        'alert_evaluation_ts': 1234,
        'alert_def': {
            'name': 'Alert',
            'team': 'team-1',
            'id': 123,
            'responsible_team': 'zmon',
            'priority': 3,
            'tags': ['tag-1'],
        },
    }

    NotifyOpsgenie._config = {
        'notifications.opsgenie.apikey': API_KEY,
        'zmon.host': 'https://zmon.example.org/'
    }

    r = NotifyOpsgenie.notify(alert, message=MESSAGE, per_entity=True, teams='team-1')

    data = {
        'alias': 'ZMON-123-e-1',
        'message': MESSAGE,
        'description': '',
        'source': 'worker-1',
        'note': 'https://zmon.example.org/#/alert-details/123',
        'entity': 'e-1',
        'details': {
            'worker': alert['worker'],
            'zmon_team': alert['alert_def']['team'],
            'entity': alert['entity']['id'],
            'infrastructure_account': 'UNKNOWN',
            'alert_evaluation_ts': 1234,
            'alert_url': 'https://zmon.example.org/#/alert-details/123',
            'owning_team': alert['alert_def']['responsible_team'],
            'application': alert['entity']['application']
        },
        'priority': 'P3',
        'tags': ['tag-1', 123],
        'teams': [{'name': 'team-1'}],
    }

    assert r == 0

    post.assert_called_with(URL_CREATE, data=json.dumps(data, cls=JsonDataEncoder, sort_keys=True), headers=HEADERS,
                            timeout=5, params={})


def test_opsgenie_notification_no_change(monkeypatch):
    alert = {
        'is_alert': True, 'alert_changed': False, 'entity': {'id': 'e-1'}, 'worker': 'worker-1',
        'alert_def': {'name': 'Alert', 'team': 'zmon', 'responsible_team': 'zmon', 'id': 123, 'priority': 1},
    }

    NotifyOpsgenie._config = {
        'notifications.opsgenie.apikey': API_KEY,
        'zmon.host': 'https://zmon.example.org/'
    }

    r = NotifyOpsgenie.notify(alert, message=MESSAGE, per_entity=False, teams='team-1', repeat=55)

    assert r == 55


def test_opsgenie_notification_error_api_key(monkeypatch):
    NotifyOpsgenie._config = {}

    alert = {
        'alert_changed': True, 'changed': True, 'is_alert': True, 'entity': {'id': 'e-1'}, 'worker': 'worker-1',
        'alert_evaluation_ts': 1234,
        'alert_def': {
            'name': 'Alert',
            'team': 'zmon',
            'responsible_team': 'zmon',
            'id': 123,
            'priority': 1,
        }
    }

    with pytest.raises(NotificationError):
        NotifyOpsgenie.notify(alert, message=MESSAGE)


def test_opsgenie_notification_error_teams(monkeypatch):
    NotifyOpsgenie._config = {'notifications.opsgenie.apikey': API_KEY}

    alert = {
        'alert_changed': True, 'changed': True, 'is_alert': True, 'entity': {'id': 'e-1'}, 'worker': 'worker-1',
        'alert_evaluation_ts': 1234,
        'alert_def': {
            'name': 'Alert',
            'team': 'zmon',
            'responsible_team': 'zmon',
            'id': 123,
            'priority': 1,
        }
    }

    with pytest.raises(NotificationError):
        NotifyOpsgenie.notify(alert, message=MESSAGE)

    with pytest.raises(NotificationError):
        NotifyOpsgenie.notify(alert, teams='team-1', message=MESSAGE, priority='p1')


def test_opsgenie_notification_exception(monkeypatch):
    post = MagicMock()
    post.side_effect = Exception
    monkeypatch.setattr('requests.post', post)

    alert = {
        'alert_changed': True, 'changed': True, 'is_alert': True, 'entity': {'id': 'e-1'}, 'worker': 'worker-1',
        'alert_def': {'name': 'Alert', 'team': 'zmon', 'responsible_team': 'zmon', 'id': 123, 'priority': 1},
    }

    NotifyOpsgenie._config = {'notifications.opsgenie.apikey': API_KEY}

    r = NotifyOpsgenie.notify(alert, message=MESSAGE, per_entity=True, teams='team-1')

    assert r == 0

    resp = requests.Response()
    resp.status_code = 400
    post.side_effect = requests.HTTPError(response=resp)
    monkeypatch.setattr('requests.post', post)

    r = NotifyOpsgenie.notify(alert, message=MESSAGE, per_entity=True, teams='team-1')

    assert r == 0
