from notification import BaseNotification

import json
import requests
import logging

logger = logging.getLogger(__name__)


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
                "title": 'Alert Ended' if alert and not alert.get('is_alert') else 'Alert Started',
                "body": kwargs.get("message", cls._get_subject(alert))
            },
            "alert_id": alert['alert_def']['id'],
            "entity_id": alert['entity']['id']
        }

        url = url + '/api/v1/publish'

        try:
            # logger.info("Sending push notification to %s %s", url, message)
            r = requests.post(url, headers={"Authorization": "PreShared " + key, 'Content-Type': 'application/json'},
                              data=json.dumps(message))
            r.raise_for_status()
        except Exception as ex:
            logger.exception("Push write failed %s", ex)

        return repeat


if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.INFO)
    NotifyPush.notify(
        alert={"id": 1048, "changed": True, "is_alert": True, "alert_def": {"name": "Database master connection"},
               "entity": {"id": "test-entity"}}, url=sys.argv[1], key=sys.argv[2])
