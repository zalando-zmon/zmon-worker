#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
import json
import sys

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.kairosdb-function')

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

    def __init__(self, url):
        self.url = url

    def query(self, name, group_by = [], tags = None, start = -5, end = 0, time_unit='seconds', aggregators = None):
        url = self.url + '/api/v1/datapoints/query'
        q = {
            "start_relative": {
                "value": start,
                "unit": time_unit
            },
            "metrics": [{
                "name": name,
            }]
        }

        if aggregators is not None:
            q["metrics"][0]["aggregators"] = aggregators

        if tags is not None:
            q["metrics"][0]["tags"] = tags

        try:
            response = requests.post(url, json=q)
            if response.status_code == requests.codes.ok:
                return response.json()["queries"][0]
            else:
                raise Exception("KairosDB Query failed: " + json.dumps(q))
        except requests.Timeout, e:
            raise HttpError('timeout', self.url), None, sys.exc_info()[2]
        except requests.ConnectionError, e:
            raise HttpError('connection failed', self.url), None, sys.exc_info()[2]

    def tagnames(self):
        return []

    def tagnames(self):
        return []

    def metric_tags(self):
        return {}