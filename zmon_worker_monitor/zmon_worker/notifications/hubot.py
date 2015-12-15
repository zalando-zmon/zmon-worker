#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from notification import BaseNotification
import logging

logger = logging.getLogger(__name__)


class HubotException(Exception):

    pass


class Hubot(BaseNotification):

    @classmethod
    def notify(cls, alert, queue, hubot_url, message=None, repeat=0):
        message = cls._get_subject(alert, custom_message=message)

        if '?' in hubot_url:
            raise ValueError

        post_params = {
            'event': queue,
            'data': message,
        }

        alert_id = alert.get('alert_def', {}).get('id', 0)

        try:
            r = requests.post(hubot_url, data=post_params)
            r.raise_for_status()
            logger.info('Notification sent: request to %s --> status: %s, response headers: %s, response body: %s',
                        hubot_url, r.status_code, r.headers, r.text)
        except Exception:
            logger.exception('Failed to send notification for alert %s with id %s to: %s', alert['name'], alert['id'],
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
