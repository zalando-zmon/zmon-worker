#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import requests
import logging

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.scalyr-function')

class ScalyrWrapperFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(ScalyrWrapperFactory, self).__init__()

    def configure(self, conf):
        self.read_key = conf.get('scalyr.read.key', '')
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(ScalyrWrapper, read_key=self.read_key, default_error_ts_id=factory_ctx.get('entity').get('scalyr_ts_id', None))


class ScalyrWrapper(object):

    def __init__(self, read_key, default_error_ts_id = None):
        self.numeric_url = 'https://www.scalyr.com/api/numericQuery'
        self.timeseries_url = 'https://www.scalyr.com/api/timeseriesQuery'
        self.read_key = read_key
        self.default_ts_id = default_error_ts_id

    def count(self, query, minutes=5):

        val = {
            'token': self.read_key,
            'queryType': 'numeric',
            'filter': query,
            'function': 'count',
            'startTime': str(minutes)+'m',
            'priority': 'low',
            'buckets': 1
        }

        r = requests.post(self.numeric_url, data=json.dumps(val), headers={"Content-Type": "application/json"})
        j = r.json()
        if 'values' in j:
            return j['values'][0]
        else:
            return j

    def function(self, function, query, minutes=5):

        val = {
            'token': self.read_key,
            'queryType': 'numeric',
            'filter': query,
            'function': function,
            'startTime': str(minutes)+'m',
            'priority': 'low',
            'buckets': 1
        }

        r = requests.post(self.numeric_url, data=json.dumps(val), headers={"Content-Type": "application/json"})
        j = r.json()
        if 'values' in j:
            return j['values'][0]
        else:
            return j

    def errors(self, minutes=30):
        return self.timeseries(self.default_ts_id, minutes)

    def timeseries(self, ts_id, minutes=30, buckets=1):

        val = {
            'token': self.read_key,
            'queries': [
                {'timeseriesId':ts_id,
                 'startTime': str(minutes)+'m',
                 'buckets': buckets}
            ]
        }

        r = requests.post(self.timeseries_url, data=json.dumps(val), headers={"Content-Type": "application/json"})
        j = r.json()
        if j['status'] == 'success':
            if len(j['results'][0]['values'])==1:
                return j['results'][0]['values'][0] * minutes
            return map(lambda x: x * minutes / buckets, j['results'][0]['values'])
        return j


if __name__ == '__main__':
    import os
    s = ScalyrWrapper(read_key=os.getenv('SCALYR_READ_KEY'))
    print s.count(query="$application_id='zmon-scheduler'")

