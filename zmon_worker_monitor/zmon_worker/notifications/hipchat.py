import logging
import urllib
import json

import requests

from urllib2 import urlparse

from notification import BaseNotification

logger = logging.getLogger(__name__)


class NotifyHipchat(BaseNotification):
    @classmethod
    def notify(cls, alert, *args, **kwargs):
        url = cls._config.get('notifications.hipchat.url')
        token = kwargs.get('token', cls._config.get('notifications.hipchat.token'))
        repeat = kwargs.get('repeat', 0)
        notify = kwargs.get('notify', False)

        color = 'green' if alert and not alert.get('is_alert') else kwargs.get('color', 'red')

        zmon_host = kwargs.get('zmon_host', cls._config.get('zmon.host'))
        alert_url = urlparse.urljoin(zmon_host, '/#/alert-details/{}'.format(alert['alert_def']['id'])) if zmon_host else ''
        message_text = cls._get_subject(alert, custom_message=kwargs.get('message'))
        if kwargs.get('link', False):
            message_text += ' -- <a href="{}">GO TO ALERT</a>'.format(alert_url)

        message = {
            'message': message_text,
            'color': color,
            'notify': notify
        }

        try:
            logger.info(
                'Sending to: ' + '{}/v2/room/{}/notification?auth_token={}'.format(url, urllib.quote(kwargs['room']),
                                                                                   token) + ' ' + json.dumps(message))
            r = requests.post(
                '{}/v2/room/{}/notification'.format(url, urllib.quote(kwargs['room'])),
                json=message, params={'auth_token': token}, headers={'Content-type': 'application/json'})
            r.raise_for_status()
        except:
            logger.exception('Hipchat write failed!')

        return repeat
