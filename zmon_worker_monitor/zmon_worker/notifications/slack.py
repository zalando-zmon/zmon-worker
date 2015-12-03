from notification import BaseNotification

import requests
import logging
logger = logging.getLogger(__name__)

class NotifySlack(BaseNotification):

    @classmethod
    def send(cls, alert, *args, **kwargs):
        url = "https://slack.com/api/chat.postMessage"
        token = kwargs.get('token', cls._config.get('notifications.slack.token'))
        repeat = kwargs.get('repeat', 0)

        message = {"as_user":"true", "token": token, "channel": kwargs.get('channel', '#general'), "text": kwargs.get("message", cls._get_subject(alert))}

        try:
            logger.info("Sending to %s %s", url, message)
            r = requests.post(url, params=message, verify=False)
            r.raise_for_status()
        except Exception as ex:
            logger.exception("Slack write failed %s", ex)

        return repeat
