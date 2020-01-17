from mock import MagicMock

from datetime import datetime

from zmon_worker_monitor.zmon_worker.notifications.google_hangouts_chat import NotifyGoogleHangoutsChat


HEADERS = {
    'Content-type': 'application/json',
}

NotifyGoogleHangoutsChat._config = {'zmon.host': 'https://zmon.example.org'}


def test_google_hangouts_chat_notification(monkeypatch):
    post = MagicMock()
    monkeypatch.setattr('requests.post', post)

    alert = {'changed': True, 'is_alert': True, 'alert_def': {'id': 123, 'name': 'alert'}, 'entity': {'id': 'e-1'}}

    URL = 'http://chat.example.org/v1/spaces/XYZ/messages?threadKey={}&key=123&token=ABC'

    webhook_link = 'http://chat.example.org/v1/spaces/XYZ/messages?key=123&token=ABC'

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

    r = NotifyGoogleHangoutsChat.notify(alert,
                                        message='ALERT',
                                        webhook_link=webhook_link)

    assert r == 0
    post.assert_called_with(URL.format("123"), json=data, headers=HEADERS, timeout=5)

    r2 = NotifyGoogleHangoutsChat.notify(alert,
                                         message='ALERT',
                                         threading='alert-date',
                                         webhook_link=webhook_link)

    assert r2 == 0
    date = str(datetime.date(datetime.now()))
    post.assert_called_with(URL.format("123" + date), json=data, headers=HEADERS, timeout=5)

    r3 = NotifyGoogleHangoutsChat.notify(alert,
                                         message='ALERT',
                                         threading='date',
                                         webhook_link=webhook_link)
    assert r3 == 0
    post.assert_called_with(URL.format(date), json=data, headers=HEADERS, timeout=5)

    r4 = NotifyGoogleHangoutsChat.notify(alert,
                                         message='ALERT',
                                         threading='none',
                                         webhook_link=webhook_link)

    assert r4 == 0
    post.assert_called_with(webhook_link, json=data, headers=HEADERS, timeout=5)
