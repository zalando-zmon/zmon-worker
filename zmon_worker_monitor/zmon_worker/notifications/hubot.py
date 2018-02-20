#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import logging

from opentracing_utils import trace, extract_span_from_kwargs

from notification import BaseNotification

logger = logging.getLogger(__name__)


class HubotException(Exception):

    pass


class Hubot(BaseNotification):

    @classmethod
    @trace(operation_name='notification_hubot', pass_span=True, tags={'notification': 'hubot'})
    def notify(cls, alert, queue, hubot_url, message=None, repeat=0, **kwargs):

        current_span = extract_span_from_kwargs(**kwargs)

        message = cls._get_subject(alert, custom_message=message)

        alert_def = alert['alert_def']
        current_span.set_tag('alert_id', alert_def['id'])

        entity = alert.get('entity')
        is_changed = alert.get('alert_changed', False)
        is_alert = alert.get('is_alert', False)

        current_span.set_tag('entity', entity['id'])
        current_span.set_tag('alert_changed', bool(is_changed))
        current_span.set_tag('is_alert', is_alert)

        if '?' in hubot_url:
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'Invalid URL!'})
            raise ValueError

        post_params = {
            'event': queue,
            'data': message,
        }

        try:
            r = requests.post(hubot_url, data=post_params)
            r.raise_for_status()
            logger.info('Notification sent: request to %s --> status: %s, response headers: %s, response body: %s',
                        hubot_url, r.status_code, r.headers, r.text)
        except Exception as e:
            current_span.set_tag('error', True)
            current_span.log_kv({'exception': str(e)})
            logger.exception(
                'Failed to send notification for alert %s with id %s to: %s', alert_def['name'], alert_def['id'],
                hubot_url)
        finally:
            return repeat


if __name__ == '__main__':

    fake_alert = {
        'is_alert': True,
        'alert_def': {'name': 'Test'},
        'entity': {'id': 'hostxy'},
        'captures': {},
    }

    Hubot.notify(fake_alert, queue='syslog.info', hubot_url='http://z-hyp18.zalando:8081/publish')
