import logging

import requests

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.errors import NotificationError
from notification import BaseNotification


logger = logging.getLogger(__name__)


class NotifySlack(BaseNotification):
    @classmethod
    def notify(cls, alert, *args, **kwargs):
        url = kwargs.get('webhook', cls._config.get('notifications.slack.webhook'))
        repeat = kwargs.get('repeat', 0)

        if not url:
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
        except Exception:
            logger.exception('Slack notification failed!')

        return repeat
