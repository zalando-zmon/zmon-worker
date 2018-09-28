from mock import MagicMock

from zmon_worker_monitor.zmon_worker.notifications.google_hangouts_chat import NotifyGoogleHangoutsChat


HEADERS = {
    'Content-type': 'application/json',
}

NotifyGoogleHangoutsChat._config = {'zmon.host': 'https://zmon.example.org'}


def test_google_hangouts_chat_notification(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 123, 'name': 'alert'}, 'entity': {'id': 'e-1'}}

    r = NotifyGoogleHangoutsChat.notify(alert,
                                        message='ALERT',
                                        webhook_link='http://chat.example.org/v1/spaces/XYZ/messages?key=123&token=ABC')

    data = {
        "cards": [{
            "sections": [{
                "widgets": [{
                    "keyValue": {
                        "onClick": {
                            "openLink": {
                                "url": "https://zmon.example.org/#/alert-details/123"
                            }
                        },
                        "icon": "FLIGHT_DEPARTURE",
                        "contentMultiline": True,
                        "content": "<font color=\"#FF0000\">NEW ALERT: ALERT!</font>"
                    }
                }]
            }]
        }]
    }

    assert r == 0

    URL = 'http://chat.example.org/v1/spaces/XYZ/messages?threadKey=123&key=123&token=ABC'

    post.assert_called_with(URL, json=data, headers=HEADERS, timeout=5)
