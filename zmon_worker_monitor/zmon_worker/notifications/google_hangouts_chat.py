import logging
import json
import traceback

import requests

from datetime import datetime

from urllib2 import urlparse

from opentracing_utils import trace, extract_span_from_kwargs

from notification import BaseNotification

logger = logging.getLogger(__name__)


class NotifyGoogleHangoutsChat(BaseNotification):
    @classmethod
    @trace(operation_name='notification_google_hangouts_chat',
           pass_span=True,
           tags={'notification': 'google_hangouts_chat'})
    def notify(cls, alert, *args, **kwargs):

        current_span = extract_span_from_kwargs(**kwargs)

        webhook_link = kwargs.get('webhook_link', 'http://no.webhook.link?wrong')
        multiline = kwargs.get('multiline', True)
        webhook_link_split = webhook_link.split('?')
        alert_id = alert['alert_def']['id']
        threading = kwargs.get('threading', 'alert')
        thread_key = cls.get_thread_key(threading, alert_id)

        webhook_link = webhook_link_split[0] + thread_key + webhook_link_split[1]

        repeat = kwargs.get('repeat', 0)
        alert_def = alert['alert_def']

        current_span.set_tag('alert_id', alert_def['id'])

        entity = alert.get('entity')
        is_changed = alert.get('alert_changed', False)
        is_alert = alert.get('is_alert', False)

        current_span.set_tag('entity', entity['id'])
        current_span.set_tag('alert_changed', bool(is_changed))
        current_span.set_tag('is_alert', is_alert)

        current_span.log_kv({'room': kwargs.get('room')})

        color = '#0CB307' if alert and not alert.get('is_alert') else kwargs.get('color', '#FF0000')
        logo = 'FLIGHT_ARRIVAL' if alert and not alert.get('is_alert') else kwargs.get('logo', 'FLIGHT_DEPARTURE')

        message_text = cls._get_subject(alert, custom_message=kwargs.get('message'))

        zmon_host = kwargs.get('zmon_host', cls._config.get('zmon.host'))
        alert_url = urlparse.urljoin(zmon_host, '/#/alert-details/{}'.format(alert_id)) if zmon_host else ''

        message = {
            "cards": [
                {
                    "sections": [
                        {
                            "widgets": [
                                {
                                    "keyValue": {
                                        "content": '<font color="{}">{}!</font>'.format(color, message_text),
                                        "contentMultiline": multiline,
                                        "onClick": {
                                             "openLink": {
                                                "url": "{}".format(alert_url)
                                             }
                                         },
                                        "icon": "{}".format(logo)
                                     }
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        try:
            logger.info(
                'Sending to: ' + '{}'.format(webhook_link) + ' ' + json.dumps(message))
            r = requests.post(
                '{}'.format(webhook_link),
                json=message,
                headers={'Content-type': 'application/json'},
                timeout=5)
            r.raise_for_status()
        except Exception:
            current_span.set_tag('error', True)
            current_span.log_kv({'exception': traceback.format_exc()})
            logger.exception('Google Hangouts Chat write failed!')

        return repeat

    @staticmethod
    def get_thread_key(threading, alert_id):
        thread_key = '?threadKey={}&'
        if threading == 'alert':
            return thread_key.format(alert_id)
        elif threading == 'date':
            date = str(datetime.date(datetime.now()))
            return thread_key.format(date)
        elif threading == 'alert-date':
            date = str(datetime.date(datetime.now()))
            return thread_key.format(str(alert_id) + date)
        elif threading == 'none':
            return '?'
        else:
            return thread_key.format(alert_id)
