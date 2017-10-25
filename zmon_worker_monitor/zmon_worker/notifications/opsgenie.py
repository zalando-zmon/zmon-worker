import json
import logging

import requests

from urllib2 import urlparse

from zmon_worker_monitor.zmon_worker.encoder import JsonDataEncoder

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from zmon_worker_monitor.zmon_worker.errors import NotificationError

from notification import BaseNotification


PRIORITIES = ('P1', 'P2', 'P3', 'P4', 'P5')


logger = logging.getLogger(__name__)


class NotifyOpsgenie(BaseNotification):
    @classmethod
    def notify(cls, alert, teams=None, per_entity=False, include_alert=True, priority=None, message='', **kwargs):
        url = 'https://api.opsgenie.com/v2/alerts'

        repeat = kwargs.get('repeat', 0)

        # Auth key!
        api_key = kwargs.get('api_key', cls._config.get('notifications.opsgenie.apikey'))
        zmon_host = kwargs.get('zmon_host', cls._config.get('zmon.host'))

        if not api_key:
            raise NotificationError('API key is required!')

        if not isinstance(teams, (list, basestring)):
            raise NotificationError('Missing "teams" parameter. Either a team name or list of team names is required.')

        if priority and priority not in PRIORITIES:
            raise NotificationError('Invalid priority. Valid values are: {}'.format(PRIORITIES))

        if teams and isinstance(teams, basestring):
            teams = [{'name': teams}]
        else:
            teams = [{'name': t} for t in teams]

        entity = alert.get('entity')
        is_changed = alert.get('alert_changed')
        is_alert = alert.get('is_alert')

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
                'entity': entity['id'],
                'note': note,
                'priority': priority,
                'tags': alert['alert_def'].get('tags', [])
            }

            if include_alert:
                data['details'] = details
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
            logger.error('HTTP Error ({}) {}'.format(e.response.status_code, e.response.text))
        except Exception:
            logger.exception('Notifying Opsgenie failed')

        return repeat
