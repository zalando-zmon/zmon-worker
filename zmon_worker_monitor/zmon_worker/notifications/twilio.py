"""
This triggers the notification service, where we temporarily store some details about the alert in question.

Notification service triggers Twilio to make the phone call, all Twilio requests go to notification service.
"""

import logging
import json

import requests
import tokens

from opentracing_utils import trace, extract_span_from_kwargs

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from notification import BaseNotification

logger = logging.getLogger(__name__)

tokens.configure()
tokens.manage('uid', ['uid'])


class NotifyTwilio(BaseNotification):

    @classmethod
    @trace(operation_name='notification_http', pass_span=True, tags={'notification': 'twilio'})
    def notify(cls, alert, *args, **kwargs):

        current_span = extract_span_from_kwargs(**kwargs)

        repeat = kwargs.get('repeat', 0)
        oauth2 = kwargs.get('oauth2', True)
        headers = {'Content-type': 'application/json'}
        timeout = 5

        alert_def = alert['alert_def']
        current_span.set_tag('alert_id', alert_def['id'])

        entity = alert.get('entity')
        is_changed = alert.get('alert_changed', False)
        is_alert = alert.get('is_alert', False)

        current_span.set_tag('entity', entity['id'])
        current_span.set_tag('alert_changed', bool(is_changed))
        current_span.set_tag('is_alert', is_alert)

        url = cls._config.get('notifications.service.url', None)
        if not url:
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'No notification service url set!'})
            logger.error('No notification service url set')
            return repeat

        url = url + '/api/v1/twilio'

        if oauth2:
            headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})
        else:
            key = kwargs.get('key', cls._config.get('notifications.service.key'))
            headers.update({'Authorization': 'Bearer {}'.format(key)})

        headers['User-Agent'] = get_user_agent()

        data = {
            'message': kwargs.get('message', cls._get_subject(alert)),
            'escalation_team': kwargs.get('team', alert['alert_def'].get('team', '')),
            'numbers': kwargs.get('numbers', []),
            'voice': kwargs.get('voice', 'woman'),
            'alert_id': alert['alert_def']['id'],
            'entity_id': alert['entity']['id'],
            'event_type': 'ALERT_ENDED' if alert and not alert.get('is_alert') else 'ALERT_START',
            'alert_changed': alert.get('alert_changed', False),
        }

        try:
            logger.info('Sending HTTP POST request to {}'.format(url))
            r = requests.post(url, data=json.dumps(data, cls=JsonDataEncoder), headers=headers, timeout=timeout)

            r.raise_for_status()
        except Exception:
            logger.exception('Twilio Request failed!')

        return repeat
