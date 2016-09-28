import json
import logging

import requests

from urllib2 import urlparse

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.errors import NotificationError

from notification import BaseNotification


logger = logging.getLogger(__name__)


class NotifyPagerduty(BaseNotification):
    @classmethod
    def notify(cls, alert, per_entity=False, include_alert=True, message='', repeat=0, **kwargs):
        url = 'https://events.pagerduty.com/generic/2010-04-15/create_event.json'

        repeat = kwargs.get('repeat', 0)

        # Auth key!
        service_key = kwargs.get('service_key', cls._config.get('notifications.pagerduty.servicekey'))
        zmon_host = kwargs.get('zmon_host', cls._config.get('zmon.host'))

        if not service_key:
            raise NotificationError('Service key is required!')

        entity = alert.get('entity')
        is_alert = alert.get('is_alert')
        event_type = 'trigger' if is_alert else 'resolve'

        alert_id = alert['alert_def']['id']
        key = 'ZMON-{}'.format(alert_id) if not per_entity else 'ZMON-{}-{}'.format(alert_id, entity['id'])

        description = message if message else cls._get_subject(alert)

        message = {
            'service_key': service_key,
            'event_type': event_type,
            'incident_key': key,
            'description': description,
            'client': 'ZMON',
            'client_url': urlparse.urljoin(zmon_host, '/#/alert-details/{}'.format(alert_id)) if zmon_host else '',
            'details': json.dumps(alert, cls=JsonDataEncoder) if include_alert else '',
        }

        try:
            logger.info('Sending to %s %s', url, message)
            headers = {'User-Agent': get_user_agent(), 'Content-type': 'application/json'}

            r = requests.post(url, json=message, headers=headers, timeout=5)

            r.raise_for_status()
        except Exception as ex:
            logger.exception('Notifying Pagerduty failed %s', ex)

        return repeat
