import logging
import json

import requests
import tokens

from opentracing_utils import trace, extract_span_from_kwargs

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder
from zmon_worker_monitor.zmon_worker.errors import NotificationError
from zmon_worker_monitor.zmon_worker.common.http import is_absolute_http_url, get_user_agent

from notification import BaseNotification

logger = logging.getLogger(__name__)


tokens.configure()
tokens.manage('uid', ['uid'])


class NotifyHttp(BaseNotification):
    @classmethod
    @trace(operation_name='notification_http', pass_span=True, tags={'notification': 'http'})
    def notify(cls, alert, url=None, body=None, params=None, headers=None, timeout=5, oauth2=False, include_alert=True,
               repeat=0, **kwargs):

        current_span = extract_span_from_kwargs(**kwargs)

        urls = cls._config.get('notifications.http.whitelist.urls', [])
        allow_any = cls._config.get('notifications.http.allow.all', False)
        default_url = cls._config.get('notifications.http.default.url', None)

        alert_def = alert['alert_def']
        current_span.set_tag('alert_id', alert_def['id'])

        entity = alert.get('entity', {})
        is_changed = alert.get('alert_changed', False)
        is_alert = alert.get('is_alert', False)

        current_span.set_tag('entity', entity.get('id'))
        current_span.set_tag('alert_changed', bool(is_changed))
        current_span.set_tag('is_alert', is_alert)

        if isinstance(urls, basestring):
            urls = urls.replace(' ', '').split(',')

        if not url and not default_url:
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'Missing URL!'})
            raise NotificationError('URL is required!')

        if not url:
            url = default_url
        elif not allow_any and url not in urls:
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'URL is not in whitelist'})
            raise NotificationError('URL "{}" is not allowed. Please check worker white list URLs.'.format(url))

        if not is_absolute_http_url(url):
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'Absolute URL required!'})
            raise NotificationError('Absolute URL is required!')

        # HTTP headers.
        if not headers:
            headers = {}

        default_headers = cls._config.get('notifications.http.headers', {})
        default_headers.update(headers)

        if oauth2:
            headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})

        headers['User-Agent'] = get_user_agent()

        if include_alert:
            data = {
                'alert': alert,
                'body': body,
            }
        else:
            data = body

        try:
            logger.info('Sending HTTP POST request to {}'.format(url))
            r = requests.post(url, data=json.dumps(data, cls=JsonDataEncoder), params=params,
                              headers=headers, timeout=timeout)

            r.raise_for_status()
        except Exception:
            current_span.set_tag('error', True)
            logger.exception('Request failed!')

        return repeat
