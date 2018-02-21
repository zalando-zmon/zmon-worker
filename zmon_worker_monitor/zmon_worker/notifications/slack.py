import logging

import requests

from opentracing_utils import trace, extract_span_from_kwargs

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.errors import NotificationError

from notification import BaseNotification


logger = logging.getLogger(__name__)


class NotifySlack(BaseNotification):
    @classmethod
    @trace(operation_name='notification_slack', pass_span=True, tags={'notification': 'slack'})
    def notify(cls, alert, *args, **kwargs):

        current_span = extract_span_from_kwargs(**kwargs)

        alert_def = alert['alert_def']
        current_span.set_tag('alert_id', alert_def['id'])

        entity = alert.get('entity')
        is_changed = alert.get('alert_changed', False)
        is_alert = alert.get('is_alert', False)

        current_span.set_tag('entity', entity['id'])
        current_span.set_tag('alert_changed', bool(is_changed))
        current_span.set_tag('is_alert', is_alert)

        url = kwargs.get('webhook', cls._config.get('notifications.slack.webhook'))
        repeat = kwargs.get('repeat', 0)

        current_span.log_kv({'channel': kwargs.get('channel')})

        if not url:
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'Missing webhook!'})
            raise NotificationError('Webhook is required!')

        message = {
            'username': 'ZMON',
            'channel': kwargs.get('channel', '#general'),
            'text': kwargs.get('message', cls._get_subject(alert)),
            'icon_emoji': ':bar_chart:',
        }

        headers = {
            'User-agent': get_user_agent(),
            'Content-type': 'application/json',
        }

        try:
            logger.info('Sending to %s %s', url, message)
            r = requests.post(url, json=message, headers=headers, timeout=5)
            r.raise_for_status()
        except Exception as e:
            current_span.set_tag('error', True)
            current_span.log_kv({'exception': str(e)})
            logger.exception('Slack notification failed!')

        return repeat
