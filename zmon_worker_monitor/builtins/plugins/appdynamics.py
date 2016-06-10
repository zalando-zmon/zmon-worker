#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import logging
import time

from datetime import datetime, timedelta

import requests

import tokens

from zmon_worker_monitor.zmon_worker.errors import HttpError
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

from zmon_worker_monitor.builtins.plugins.elasticsearch import ElasticsearchWrapper


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

SOURCE_TYPE_APPLICATION_LOG = 'application-log'


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
        self._url = conf.get('url')
        self._es_url = conf.get('es_url')
        self._index_prefix = conf.get('index_prefix')

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(
            AppdynamicsWrapper, url=self._url, username=self._user, password=self._pass, es_url=self._es_url,
            index_prefix=self._index_prefix)


class AppdynamicsWrapper(object):
    def __init__(self, url=None, username=None, password=None, es_url=None, index_prefix=''):
        if not url:
            raise RuntimeError('AppDynamics plugin improperly configured. URL is required!')

        self.url = url
        self.es_url = es_url

        self.index_prefix = index_prefix

        self.__oauth2 = False

        self.__session = requests.Session()

        if not username or not password:
            self.__oauth2 = True
            self.__session.headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})
        else:
            self.__session.auth = (username, password)

        self.__session.headers.update({'User-Agent': get_user_agent()})
        self.__session.params = {'output': 'json'}
        self.__session.timeout = 3

    def healthrule_violations_url(self, application):
        return os.path.join(self.url, 'applications', application, 'problems', 'healthrule-violations')

    def healthrule_violations(self, application, time_range_type=BEFORE_NOW, duration_in_mins=5, start_time=None,
                              end_time=None, severity=None):
        """
        Return Healthrule violations for AppDynamics application.

        :param application: Application name or ID
        :type application: str

        :param time_range_type: Valid time range type. Valid range types are BEFORE_NOW, BEFORE_TIME, AFTER_TIME and
                                BETWEEN_TIMES. Default is BEFORE_NOW.
        :type time_range_type: str

        :param duration_in_mins: Time duration in mins. Required for BEFORE_NOW, AFTER_TIME, BEFORE_TIME range types.
        :type duration_in_mins: int

        :param start_time: Start time (in milliseconds) from which the metric data is returned. Default is 5 mins ago.
        :type start_time: int

        :param end_time: End time (in milliseconds) until which the metric data is returned. Default is now.
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
                    start_time = int(time.mktime((datetime.utcnow() - timedelta(minutes=5)).timetuple())) * 1000
                params['start-time'] = start_time

            if time_range_type in (BEFORE_TIME, BETWEEN_TIMES):
                if end_time is None:
                    end_time = int(time.time()) * 1000
                params['end-time'] = end_time

            params['time-range-type'] = time_range_type

            resp = self.__session.get(self.healthrule_violations_url(application), params=params)

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

    def query_logs(self, q='', body=None, size=100, source_type=SOURCE_TYPE_APPLICATION_LOG):
        """
        Perform search query on AppDynamics ES logs.

        :param q: Query string used in search.
        :type q: str

        :param body: (dict) holding an ES query DSL.
        :type body: dict

        :param size: Number of hits to return. Default is 100.
        :type size: int

        :param source_type: ``sourceType`` field filtering. Default to application-log, and will be part of ``q``.
        :type source_type: str

        :return: ES query result ``hits``.
        :rtype: list
        """
        if not self.es_url:
            raise RuntimeError('AppDynamics plugin improperly configured. ES URL is required to query logs!')

        q = '{} sourceType:{}'.format(q, source_type)
        indices = ['{}*'.format(self.index_prefix)]

        res = (ElasticsearchWrapper(url=self.es_url, oauth2=self.__oauth2)
               .search(indices=indices, q=q, body=body, size=size))

        return res['hits']['hits']

    def count_logs(self, q='', body=None, source_type=SOURCE_TYPE_APPLICATION_LOG):
        """
        Perform count query on AppDynamics ES logs.

        :param q: Query string used in search.
        :type q: str

        :param body: (dict) holding an ES query DSL.
        :type body: dict

        :param source_type: ``sourceType`` field filtering. Default to application-log, and will be part of ``q``.
        :type source_type: str

        :return: Query match count.
        :rtype: int
        """
        if not self.es_url:
            raise RuntimeError('AppDynamics plugin improperly configured. ES URL is required to query logs!')

        q = '{} sourceType:{}'.format(q, source_type)

        indices = ['{}*'.format(self.index_prefix)]

        res = ElasticsearchWrapper(url=self.es_url, oauth2=self.__oauth2).count(indices=indices, q=q, body=body)

        logger.debug('Received ES count result: {}'.format(res))

        return res['count']
