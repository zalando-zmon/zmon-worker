#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
import json
import sys
import os

import tokens

from zmon_worker_monitor.zmon_worker.errors import HttpError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.kairosdb-function')


DATAPOINTS_ENDPOINT = '/api/v1/datapoints/query'


# will use OAUTH2_ACCESS_TOKEN_URL environment variable by default
# will try to read application credentials from CREDENTIALS_DIR
tokens.configure()
tokens.manage('uid', ['uid'])
tokens.start()


class KairosdbFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(KairosdbFactory, self).__init__()

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(KairosdbWrapper, url=factory_ctx.get('entity_url'))


class KairosdbWrapper(object):
    def __init__(self, url, oauth2=False):
        self.url = url

        self.__session = requests.Session()

        if oauth2:
            self.__session.headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})

    def query(self, name, group_by=None, tags=None, start=-5, end=0, time_unit='seconds', aggregators=None):
        """
        Query kairosdb.

        :param name: Metric name.
        :type name: str

        :param group_by: List of fields to group by. Currently ignored.
        :type group_by: list

        :param tags: Filtering tags.
        :type tags: dict

        :param start: Relative start time. Default is -5.
        :type start: int

        :param end: End time. Default is 0.
        :type end: int

        :param time_unit: Time unit ('seconds', 'minutes', 'hours'). Default is 'seconds'
        :type time_unit: str.

        :param aggregators: List of aggregators.
        :type aggregators: list

        :return: Result queries.
        :rtype: dict
        """
        url = os.path.join(self.url, DATAPOINTS_ENDPOINT)

        if group_by is None:
            group_by = []

        q = {
            'start_relative': {
                'value': start,
                'unit': time_unit
            },
            'metrics': [{
                'name': name,
            }]
        }

        if aggregators is not None:
            q['metrics'][0]['aggregators'] = aggregators

        if tags is not None:
            q['metrics'][0]['tags'] = tags

        try:
            response = self.__session.post(url, json=q)
            if response.ok:
                return response.json()['queries'][0]
            else:
                raise Exception('KairosDB Query failed: ' + json.dumps(q))
        except requests.Timeout:
            raise HttpError('timeout', self.url), None, sys.exc_info()[2]
        except requests.ConnectionError:
            raise HttpError('connection failed', self.url), None, sys.exc_info()[2]

    def tagnames(self):
        return []

    def metric_tags(self):
        return {}
