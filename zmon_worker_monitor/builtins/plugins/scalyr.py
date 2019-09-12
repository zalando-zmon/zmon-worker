#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import logging
import time

from zmon_worker_monitor.zmon_worker.errors import ConfigurationError
from zmon_worker_monitor.zmon_worker.errors import CheckError
from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.scalyr-function')

SCALYR_URL_PREFIX_US = 'https://www.scalyr.com/api'
SCALYR_URL_PREFIX_EU = 'https://eu.scalyr.com/api'


class ScalyrWrapperFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(ScalyrWrapperFactory, self).__init__()

    def configure(self, conf):
        self.read_key = conf.get('read.key', '')
        self.scalyr_region = conf.get('scalyr.region', '')
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(ScalyrWrapper, read_key=self.read_key, scalyr_region=self.scalyr_region)


class ScalyrWrapper(object):
    def __init__(self, read_key, scalyr_region=None):
        scalyr_prefix = SCALYR_URL_PREFIX_US

        if scalyr_region == 'eu':
            scalyr_prefix = SCALYR_URL_PREFIX_EU

        self.__query_url = '{}/query'.format(scalyr_prefix)
        self.__numeric_url = '{}/numericQuery'.format(scalyr_prefix)
        self.__timeseries_url = '{}/timeseriesQuery'.format(scalyr_prefix)
        self.__facet_url = '{}/facetQuery'.format(scalyr_prefix)
        self.__power_query_url = '{}/powerQuery'.format(scalyr_prefix)

        if not read_key:
            raise ConfigurationError('Scalyr read key is not set.')
        self.__read_key = read_key

    def count(self, query, minutes=5, align=30, end=0):
        return self.timeseries(query, function='count', minutes=minutes, buckets=1, prio='low',
                               align=align, end=end)

    def logs(self, query, max_count=100, minutes=5, continuation_token=None, columns=None, end=0):

        if not query or not query.strip():
            raise CheckError('query "{}" is not allowed to be blank'.format(query))

        val = {
            'token': self.__read_key,
            'queryType': 'log',
            'maxCount': max_count,
            'filter': query,
            'startTime': str(minutes) + 'm',
            'priority': 'low'
        }
        if end is not None:
            val['endTime'] = str(end) + 'm'

        if columns:
            val['columns'] = ','.join(columns) if type(columns) is list else str(columns)

        if continuation_token:
            val['continuationToken'] = continuation_token

        r = requests.post(self.__query_url,
                          json=val,
                          headers={'Content-Type': 'application/json', 'errorStatus': 'always200'})

        j = r.json()

        if 'matches' in j:
            new_continuation_token = j.get('continuationToken', None)
            messages = j['matches'] if columns else [match['message'] for match in j['matches']]
            return {'messages': messages, 'continuation_token': new_continuation_token}
        if j.get('status', '').startswith('error'):
            raise CheckError(j['message'])
        else:
            raise CheckError('No logs or error message was returned from scalyr')

    def function(self, function, query, minutes=5, end=0):

        val = {
            'token': self.__read_key,
            'queryType': 'numeric',
            'filter': query,
            'function': function,
            'startTime': str(minutes) + 'm',
            'priority': 'low',
            'buckets': 1
        }
        if end is not None:
            val['endTime'] = str(end) + 'm'

        r = requests.post(self.__numeric_url, json=val, headers={'Content-Type': 'application/json'})

        r.raise_for_status()

        j = r.json()
        if 'values' in j:
            return j['values'][0]
        else:
            return j

    def facets(self, filter, field, max_count=5, minutes=30, prio='low', end=0):

        val = {
            'token': self.__read_key,
            'queryType': 'facet',
            'filter': filter,
            'field': field,
            'maxCount': max_count,
            'startTime': str(minutes) + 'm',
            'priority': prio
        }
        if end is not None:
            val['endTime'] = str(end) + 'm'

        r = requests.post(self.__facet_url, json=val, headers={'Content-Type': 'application/json'})

        r.raise_for_status()

        j = r.json()
        return j

    def timeseries(self, filter, function='count', minutes=30, buckets=1, prio='low', align=30, end=0):
        start_time = str(minutes) + 'm'
        end_time = None
        if align != 0:
            cur_time = int(time.time())  # this assumes the worker is running with UTC time
            if end is not None:
                cur_time = int(time.time()) - 60 * end
            end_time = cur_time - (cur_time % align)
            start_time = end_time - (minutes * 60)
        elif end is not None:
            end_time = str(end) + 'm'

        val = {
            'token': self.__read_key,
            'queries': [
                {
                    'filter': filter,
                    'function': function,
                    'startTime': start_time,
                    'buckets': buckets,
                    'priority': prio
                }
            ]
        }
        if end_time:
            val['queries'][0]['endTime'] = end_time

        r = requests.post(self.__timeseries_url, json=val, headers={'Content-Type': 'application/json'})

        r.raise_for_status()

        j = r.json()
        if j['status'] == 'success':
            if len(j['results'][0]['values']) == 1:
                return j['results'][0]['values'][0]
            return [x * minutes / buckets for x in j['results'][0]['values']]
        return j

    def power_query(self, query, minutes=5, end=0):
        if not query or not query.strip():
            raise CheckError('query "{}" is not allowed to be blank'.format(query))

        value = {
            'token': self.__read_key,
            'query': query,
            'startTime': str(minutes) + 'm',
            'priority': 'low',
        }
        if end is not None:
            value['endTime'] = str(end) + 'm'

        response = requests.post(self.__power_query_url,
                                 json=value,
                                 headers={'Content-Type': 'application/json', 'errorStatus': 'always200'})

        json_response = response.json()

        if json_response.get('status', '').startswith('error'):
            raise CheckError(json_response.get('message', 'Unexpected error message was returned from scalyr'))

        return json_response


if __name__ == '__main__':
    import os

    s = ScalyrWrapper(read_key=os.getenv('SCALYR_READ_KEY'))
    print(s.count(query='$application_id="zmon-scheduler"'))
