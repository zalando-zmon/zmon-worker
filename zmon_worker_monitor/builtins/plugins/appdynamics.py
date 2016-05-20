#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import logging

import requests

import tokens

from zmon_worker_monitor.zmon_worker.errors import HttpError
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.appdynamics-function')


BEFORE_NOW = 'BEFORE_NOW'
BEFORE_TIME = 'BEFORE_TIME'
AFTER_TIME = 'AFTER_TIME'
BETWEEN_TIMES = 'BETWEEN_TIMES'

CRITICAL = 'CRITICAL'
WARNING = 'WARNING'

TIME_RANGE_TYPES = (
    BEFORE_NOW,
    BEFORE_TIME,
    AFTER_TIME,
    BETWEEN_TIMES,
)

SEVERITIES = (
    WARNING,
    CRITICAL,
)


# will use OAUTH2_ACCESS_TOKEN_URL environment variable by default
# will try to read application credentials from CREDENTIALS_DIR
tokens.configure()
tokens.manage('uid', ['uid'])
tokens.start()


class AppdynamicsFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(AppdynamicsFactory, self).__init__()
        # Fields should be fetched from config
        self._user = None
        self._pass = None

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        self._user = conf.get('user')
        self._pass = conf.get('pass')

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(AppdynamicsWrapper, username=self._user, password=self._pass)


class AppdynamicsWrapper(object):
    def __init__(self, url, username=None, password=None):
        self.url = url
        self.timeout = 3

        self._session = requests.Session()

        if not username or not password:
            self._session.headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})
        else:
            self._session.auth = (username, password)

        self._session.headers.update({'User-Agent': get_user_agent()})
        self._session.params = {'output': 'json'}
        self._session.timeout = 3

    @property
    def session(self):
        return self._session

    def healthrule_violations_url(self, application):
        return os.path.join(self.url, 'applications', application, 'problems', 'healthrule-violations')

    def healthrule_violations(self, application, time_range_type='', duration_in_mins=None, start_time=None,
                              end_time=None, severity=None):
        """
        Return Healthrule violations for AppDynamics application.

        :param application: Application name or ID
        :type application: str

        :param time_range_type: Valid time range type.
        :type time_range_type: str

        :param duration_in_mins: Time duration in mins. Required for BEFORE_NOW, AFTER_TIME, BEFORE_TIME range types.
        :type duration_in_mins: int

        :param start_time: Start time (in milliseconds) from which the metric data is returned.
        :type start_time: int

        :param end_time: End time (in milliseconds) until which the metric data is returned.
        :type end_time: int

        :param severity: Filter results based on severity. Valid values CRITICAL or WARNING.
        :type severity: str

        :return: List of healthrule violations
        :rtype: list
        """
        try:
            params = {}

            if time_range_type not in TIME_RANGE_TYPES:
                raise Exception('Invalid type! Allowed types are: {}'.format(','.join(TIME_RANGE_TYPES)))

            if severity is not None and severity not in SEVERITIES:
                raise Exception('Invalid severity! Allowed values are: {}'.format(','.join(SEVERITIES)))

            # Construct query params
            if time_range_type in (BEFORE_NOW, AFTER_TIME, BEFORE_TIME):
                if duration_in_mins is None:
                    raise Exception('Required "duration_in_mins" arg is missing!')
                params['duration-in-mins'] = duration_in_mins

            if time_range_type in (AFTER_TIME, BETWEEN_TIMES):
                if start_time is None:
                    raise Exception('Required "start_time" arg is missing!')
                params['start-time'] = start_time

            if time_range_type in (BEFORE_TIME, BETWEEN_TIMES):
                if end_time is None:
                    raise Exception('Required "end_time" arg is missing!')
                params['end-time'] = end_time

            params['time-range-type'] = time_range_type

            resp = self.session.get(self.healthrule_violations_url(application), params=params)

            resp.raise_for_status()

            json_resp = resp.json()

            if severity:
                # we need some filtering!
                return [e for e in json_resp if e['severity'] == severity]

            return json_resp
        except requests.Timeout:
            raise HttpError('timeout', self.url), None, sys.exc_info()[2]
        except requests.ConnectionError:
            raise HttpError('connection failed', self.url), None, sys.exc_info()[2]
        except:
            logger.exception('AppDynamics request failed')
            raise
