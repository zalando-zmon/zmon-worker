from notification import BaseNotification

import json
import requests
import logging

logger = logging.getLogger(__name__)


def formatEntity(entity_id):
    parts = entity_id.split("[")
    if len(parts) > 1:
        acc = parts[1].split(":")

        if acc[0] == "aws":
            return "{}:{}".format(parts[0], acc[1][-3:])
        if acc[0] == "dc":
            return "{}:{}".format(parts[0], acc[1])

    return parts[0]


class NotifyPush(BaseNotification):
    @classmethod
    def notify(cls, alert, *args, **kwargs):
        url = kwargs.get('url', cls._config.get('notifications.push.url'))
        key = kwargs.get('key', cls._config.get('notifications.push.key'))

        if url is None or "" == url:
            return 0

        repeat = kwargs.get('repeat', 0)

        message = {
            "notification": {
                "icon": 'clean.png' if alert and not alert.get('is_alert') else 'warning.png',
                "title": kwargs.get("message", cls._get_expanded_alert_name(alert)),
                "body": kwargs.get("body", formatEntity(alert["entity"]["id"])),
                "alert_changed": alert.get('alert_changed', False),
                "click_action": kwargs.get("click_action", "/#/alert-details/{}".format(alert["alert_def"]["id"])),
                "collapse_key": kwargs.get("collapse_key",
                                           "{}:{}".format(alert['alert_def']['id'], alert['entity']['id']))
            },
            "alert_id": alert['alert_def']['id'],
            "entity_id": alert['entity']['id'],
            "team": kwargs.get('team', alert['alert_def'].get('team', '')),
            "priority": alert["alert_def"]["priority"]
        }

        url = url + '/api/v1/publish'

        try:
            # logger.info("Sending push notification to %s %s", url, message)
            r = requests.post(url, headers={"Authorization": "PreShared " + key, 'Content-Type': 'application/json'},
                              data=json.dumps(message))
            r.raise_for_status()
        except Exception:
            pass

        return repeat


if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.INFO)
    NotifyPush.notify(
        alert={"id": 1048, "changed": True, "is_alert": True, "alert_def": {"name": "Database master connection"},
               "entity": {"id": "test-entity"}}, url=sys.argv[1], key=sys.argv[2])
