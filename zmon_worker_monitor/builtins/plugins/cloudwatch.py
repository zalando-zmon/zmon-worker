#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import collections
import datetime
import fnmatch
import logging
import requests

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logging.getLogger('botocore').setLevel(logging.WARN)


class CloudwatchWrapperFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(CloudwatchWrapperFactory, self).__init__()

    def configure(self, conf):
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(CloudwatchWrapper, region=factory_ctx.get('entity').get('region', None))


def get_region():
    r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=3)
    return r.json()['region']


def matches(dimensions, filters):
    for key, pattern in filters.items():
        if not fnmatch.fnmatch(''.join(dimensions.get(key, '')), pattern):
            return False
    return True


class CloudwatchWrapper(object):
    def __init__(self, region=None):
        if not region:
            region = get_region()
        self.__client = boto3.client('cloudwatch', region_name=region)

    def query_one(self, dimensions, metric_name, statistics, namespace, period=60, minutes=5, start=None, end=None):
        '''Query single metric statistic and return scalar value (float), all parameters need to be known in advance'''
        if period < 60 or period % 60 != 0:
            raise ValueError('Period must be greater than and a multiple of 60')

        # special case to gather all types at once
        if statistics is None:
            statistics = ['Sum', 'Average', 'Maximum', 'SampleCount', 'Minimum']
        elif isinstance(statistics, basestring):
            statistics = [statistics]

        end = end or datetime.datetime.utcnow()
        start = start or (end - datetime.timedelta(minutes=minutes))
        if isinstance(dimensions, dict):
            # transform Python dict to stupid AWS list structure
            # see http://boto3.readthedocs.org/en/latest/reference/services/cloudwatch.html#CloudWatch.Client.get_metric_statistics  # noqa
            dimensions = list({'Name': k, 'Value': v} for k, v in dimensions.items())
        response = self.__client.get_metric_statistics(Namespace=namespace, MetricName=metric_name,
                                                       Dimensions=dimensions,
                                                       StartTime=start, EndTime=end, Period=period,
                                                       Statistics=statistics)
        data_points = sorted(response['Datapoints'], key=lambda x: x["Timestamp"])
        if not data_points:
            return None
        if len(statistics) == 1:
            return data_points[-1][statistics[0]]
        else:
            return {s: v for s, v in data_points[-1].items() if s in statistics}

    def query(self, dimensions, metric_name, statistics='Sum', namespace=None, period=60, minutes=5):
        '''Query one or more metric statistics; allows finding dimensions with wildcards'''
        filter_dimension_keys = set()
        filter_dimension_pattern = {}
        for key, val in list(dimensions.items()):
            if val == 'NOT_SET':
                filter_dimension_keys.add(key)
                del dimensions[key]
            if val and '*' in val:
                filter_dimension_pattern[key] = val
                del dimensions[key]
        dimension_kvpairs = [{'Name': k, 'Value': v} for k, v in dimensions.items()]
        args = {'Dimensions': dimension_kvpairs, 'MetricName': metric_name}
        if namespace:
            args['Namespace'] = namespace

        metrics = []
        while True:
            res = self.__client.list_metrics(**args)
            metrics.extend(res['Metrics'])
            if 'NextToken' in res:
                args['NextToken'] = res['NextToken']
            else:
                break

        end = datetime.datetime.utcnow()
        start = end - datetime.timedelta(minutes=minutes)
        data = collections.defaultdict(int)
        data['dimensions'] = collections.defaultdict(int)
        for metric in metrics:
            metric_dimensions = {d['Name']: d['Value'] for d in metric['Dimensions']}
            if set(metric_dimensions.keys()) & filter_dimension_keys:
                continue
            if filter_dimension_pattern and not matches(metric_dimensions, filter_dimension_pattern):
                continue
            val = self.query_one(
                metric['Dimensions'], metric['MetricName'], statistics, metric['Namespace'], period,
                start=start, end=end)
            if val:
                for [dim_name, dim_val] in metric_dimensions.items():
                    if dim_name not in data['dimensions']:
                        data['dimensions'][dim_name] = collections.defaultdict(int)
                    data['dimensions'][dim_name][dim_val] += val
                data[metric['MetricName']] += val
        return data
