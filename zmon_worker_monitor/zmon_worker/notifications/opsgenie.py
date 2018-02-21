import time
import json
import logging

import requests

from urllib2 import urlparse

from opentracing_utils import trace, extract_span_from_kwargs

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.errors import NotificationError

from notification import BaseNotification


PRIORITIES = ('P1', 'P2', 'P3', 'P4', 'P5')


logger = logging.getLogger(__name__)


class NotifyOpsgenie(BaseNotification):
    @classmethod
    @trace(operation_name='notification_opsgenie', pass_span=True, tags={'notification': 'opsgenie'})
    def notify(cls,
               alert,
               teams=None,
               per_entity=False,
               include_alert=True,
               priority=None,
               message='',
               description='',
               **kwargs):

        current_span = extract_span_from_kwargs(**kwargs)

        url = 'https://api.opsgenie.com/v2/alerts'

        repeat = kwargs.get('repeat', 0)

        # Auth key!
        api_key = kwargs.get('api_key', cls._config.get('notifications.opsgenie.apikey'))
        zmon_host = kwargs.get('zmon_host', cls._config.get('zmon.host'))

        entity = alert.get('entity')
        is_changed = alert.get('alert_changed', False)
        is_alert = alert.get('is_alert', False)

        current_span.set_tag('entity', entity['id'])
        current_span.set_tag('alert_changed', bool(is_changed))
        current_span.set_tag('is_alert', is_alert)

        alert_def = alert['alert_def']
        current_span.set_tag('alert_id', alert_def['id'])

        if not api_key:
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'API key is required!'})
            raise NotificationError('API key is required!')

        if not isinstance(teams, (list, basestring)):
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'Missing team!'})
            raise NotificationError('Missing "teams" parameter. Either a team name or list of team names is required.')

        current_span.log_kv({'teams': teams})

        if priority and priority not in PRIORITIES:
            current_span.set_tag('notification_invalid', True)
            current_span.log_kv({'reason': 'Invalid priorities'})
            raise NotificationError('Invalid priority. Valid values are: {}'.format(PRIORITIES))

        if teams and isinstance(teams, basestring):
            teams = [{'name': teams}]
        else:
            teams = [{'name': t} for t in teams]

        if not is_changed and not per_entity:
            return repeat

        alert_id = alert['alert_def']['id']
        alias = 'ZMON-{}'.format(alert_id) if not per_entity else 'ZMON-{}-{}'.format(alert_id, entity['id'])

        note = urlparse.urljoin(zmon_host, '/#/alert-details/{}'.format(alert_id)) if zmon_host else ''

        if not priority:
            priority = 'P1' if int(alert['alert_def']['priority']) == 1 else 'P3'

        responsible_team = alert['alert_def'].get('responsible_team', teams[0]['name'])
        msg = message if message else cls._get_subject(alert, include_event=False)

        details = {
            'alert_evaluation_ts': alert.get('alert_evaluation_ts', time.time())
        }

        alert_details = {
            'worker': alert['worker'],
            'id': alert_id,
            'name': alert['alert_def']['name'],
            'team': alert['alert_def']['team'],
            'responsible_team': alert['alert_def']['responsible_team'],
            'entity': entity['id'],
            'infrastructure_account': entity.get('infrastructure_account', 'UNKNOWN'),
        }

        params = {}

        if is_alert:
            data = {
                'alias': alias,
                'teams': teams,
                'message': '[{}] - {}'.format(responsible_team, msg),  # TODO: remove when it is no longer needed!
                'source': alert.get('worker', ''),
                'description': description,
                'entity': entity['id'],
                'note': note,
                'priority': priority,
                'tags': alert['alert_def'].get('tags', []),
                'details': details,
            }

            if include_alert:
                data['details'].update(alert_details)
        else:
            logger.info('Closing Opsgenie alert {}'.format(alias))

            url = 'https://api.opsgenie.com/v2/alerts/{}/close'.format(alias)
            data = {
                'user': 'ZMON',
                'source': alert.get('worker', 'ZMON Worker'),
                'note': note,
            }

            params = {'identifierType': 'alias'}

        try:
            logger.info('Notifying Opsgenie %s %s', url, message)
            headers = {
                'User-Agent': get_user_agent(),
                'Content-type': 'application/json',
                'Authorization': 'GenieKey {}'.format(api_key),
            }

            r = requests.post(url, data=json.dumps(data, cls=JsonDataEncoder, sort_keys=True), headers=headers,
                              timeout=5, params=params)

            r.raise_for_status()
        except requests.HTTPError as e:
            current_span.set_tag('error', True)
            logger.error('HTTP Error ({}) {}'.format(e.response.status_code, e.response.text))
        except Exception as e:
            current_span.set_tag('error', True)
            current_span.log_kv({'exception': str(e)})
            logger.exception('Notifying Opsgenie failed')

        return repeat
