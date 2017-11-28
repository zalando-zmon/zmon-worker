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
        url = 'https://events.pagerduty.com/v2/enqueue'

        repeat = kwargs.get('repeat', 0)

        # Auth key!
        routing_key = kwargs.get('routing_key', cls._config.get('notifications.pagerduty.servicekey'))
        zmon_host = kwargs.get('zmon_host', cls._config.get('zmon.host'))

        if not routing_key:
            raise NotificationError('Service key is required!')

        entity = alert.get('entity')
        is_changed = alert.get('alert_changed')
        is_alert = alert.get('is_alert')

        if not is_changed and not per_entity:
            return repeat

        event_action = 'trigger' if is_alert else 'resolve'

        alert_id = alert['alert_def']['id']
        key = 'ZMON-{}'.format(alert_id) if not per_entity else 'ZMON-{}-{}'.format(alert_id, entity['id'])

        description = message if message else cls._get_subject(alert, include_event=False)

        alert_class = kwargs.get('alert_class', '')
        alert_group = kwargs.get('alert_group', '')

        message = {
            'routing_key': routing_key,
            'event_action': event_action,
            'dedup_key': key,
            'client': 'ZMON',
            'client_url': urlparse.urljoin(zmon_host, '/#/alert-details/{}'.format(alert_id)) if zmon_host else '',
            'payload': {
                'summary': description,
                'source': alert.get('worker', ''),
                'severity': 'critical' if int(alert['alert_def']['priority']) == 1 else 'error',
                'component': entity['id'],
                'custom_details': alert if include_alert else {},
                'class': alert_class,
                'group': alert_group,
            },
        }

        try:
            logger.info('Notifying Pagerduty %s %s', url, message)
            headers = {'User-Agent': get_user_agent(), 'Content-type': 'application/json'}

            r = requests.post(url, data=json.dumps(message, cls=JsonDataEncoder), headers=headers, timeout=5)

            r.raise_for_status()
        except Exception:
            logger.exception('Notifying Pagerduty failed')

        return repeat
