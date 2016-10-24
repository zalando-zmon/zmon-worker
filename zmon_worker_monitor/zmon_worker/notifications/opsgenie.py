import json
import logging

import requests

from urllib2 import urlparse

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.errors import NotificationError

from notification import BaseNotification


logger = logging.getLogger(__name__)


class NotifyOpsgenie(BaseNotification):
    @classmethod
    def notify(cls, alert, per_entity=False, include_alert=True, message='', **kwargs):
        url = 'https://api.opsgenie.com/v1/json/alert'

        repeat = kwargs.get('repeat', 0)

        # Auth key!
        api_key = kwargs.get('api_key', cls._config.get('notifications.opsgenie.apikey'))
        zmon_host = kwargs.get('zmon_host', cls._config.get('zmon.host'))

        if not api_key:
            raise NotificationError('API key is required!')

        entity = alert.get('entity')
        is_alert = alert.get('is_alert')

        alert_id = alert['alert_def']['id']
        alias = 'ZMON-{}'.format(alert_id) if not per_entity else 'ZMON-{}-{}'.format(alert_id, entity['id'])

        note = urlparse.urljoin(zmon_host, '/#/alert-details/{}'.format(alert_id)) if zmon_host else ''

        if is_alert:
            data = {
                'apiKey': api_key,
                'alias': alias,
                'message': message if message else cls._get_subject(alert),
                'source': 'ZMON',
                'entity': entity['id'],
                'note': note,
                'details': alert if include_alert else {},
            }
        else:
            logger.info('Closing Opsgenie alert {}'.format(alias))

            url = 'https://api.opsgenie.com/v1/json/alert/close'
            data = {
                'apiKey': api_key,
                'alias': alias,
                'source': 'ZMON',
                'note': note,
            }

        try:
            logger.info('Sending to %s %s', url, message)
            headers = {'User-Agent': get_user_agent(), 'Content-type': 'application/json'}

            r = requests.post(url, data=json.dumps(data, cls=JsonDataEncoder, sort_keys=True), headers=headers,
                              timeout=5)

            r.raise_for_status()
        except requests.HTTPError as e:
            logger.error('HTTP Error ({}) {}'.format(e.response.status_code, e.response.text))
        except:
            logger.exception('Notifying Opsgenie failed')

        return repeat
