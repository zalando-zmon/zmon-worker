from mock import MagicMock

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.notifications.google_hangouts_chat import NotifyGoogleHangoutsChat


HEADERS = {
    'User-agent': get_user_agent(),
    'Content-type': 'application/json',
}


def test_slack_notification(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 123, 'name': 'alert'}, 'entity': {'id': 'e-1'},
             'webhook_link': 'https://chat.googleapis.com/v1/spaces/XYZ/messages?&key=123&token=ABC'}

    r = NotifyGoogleHangoutsChat.notify(alert, message='ALERT')

    data = {
        "cards": [
            {
                "sections": [
                    {
                        "widgets": [
                            {
                                "keyValue": {
                                    "content": "<font color=\"#FF0000\">ALERT</font>",
                                    "contentMultiline": "true",
                                    "onClick": {
                                         "openLink": {
                                            "url": "https://example.com/"
                                         }
                                     },
                                    "icon": "FLIGHT_DEPARTURE"
                                 }
                            }
                        ]
                    }
                ]
            }
        ]
    }

    assert r == 0

    URL = 'https://chat.googleapis.com/v1/spaces/XYZ/messages?threadKey=123&key=123&token=ABC'

    post.assert_called_with(URL, json=data, headers=HEADERS, timeout=5)
