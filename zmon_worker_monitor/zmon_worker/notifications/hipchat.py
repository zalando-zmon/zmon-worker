
from notification import BaseNotification

import logging
import requests
import urllib
import json
logger = logging.getLogger(__name__)


class NotifyHipchat(BaseNotification):

    @classmethod
    def send(cls, alert, *args, **kwargs):
        url = cls._config.get('notifications.hipchat.url')
        token = kwargs.get('token', cls._config.get('notifications.hipchat.token'))
        repeat = kwargs.get('repeat', 0)

        color = 'green' if alert and not alert.get('is_alert') else kwargs.get("color", "red")

        message = {"message": kwargs.get("message", cls._get_subject(alert)), "color": color}

        try:
            logger.info("Sending to: " + '{}/v2/room/{}/notification?auth_token={}'.format(url, urllib.quote(kwargs['room']), token) + " " + json.dumps(message))
            r = requests.post('{}/v2/room/{}/notification?auth_token={}'.format(url, urllib.quote(kwargs['room']), token), data=json.dumps(message), verify=False, headers={'Content-type':'application/json'})
            r.raise_for_status()
        except Exception as ex:
            logger.exception("Hipchat write failed %s", ex)

        return repeat