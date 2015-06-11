#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto.ec2.cloudwatch
import boto.utils
import collections
import datetime
import fnmatch
import json
import sys

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
    identity = boto.utils.get_instance_identity()['document']
    return identity['region']


def matches(dimensions, filters):
    for key, pattern in filters.items():
        if not fnmatch.fnmatch(''.join(dimensions.get(key, '')), pattern):
            return False
    return True


class CloudwatchWrapper(object):

    def __init__(self, region=None):
        if not region:
            region = get_region()
        self.connection = boto.ec2.cloudwatch.connect_to_region(region)

    def query(self, dimensions, metric_name, statistics='Sum', namespace=None, unit=None, period=60):
        filter_dimension_keys = set()
        filter_dimension_pattern = {}
        for key, val in list(dimensions.items()):
            if val == 'NOT_SET':
                filter_dimension_keys.add(key)
                del dimensions[key]
            if val and '*' in val:
                filter_dimension_pattern[key] = val
                del dimensions[key]
        metrics = self.connection.list_metrics(dimensions=dimensions, metric_name=metric_name, namespace=namespace)
        end = datetime.datetime.utcnow()
        start = end - datetime.timedelta(minutes=5)
        data = collections.defaultdict(int)
        for metric in metrics:
            if set(metric.dimensions.keys()) & filter_dimension_keys:
                continue
            if filter_dimension_pattern and not matches(metric.dimensions, filter_dimension_pattern):
                continue
            data_points = metric.query(start, end, statistics, period=period)
            if data_points:
                data[metric.name] += data_points[-1][statistics]
        return data


if __name__ == '__main__':
    cloudwatch = CloudwatchWrapper(sys.argv[1])
    data = cloudwatch.query({'AvailabilityZone': 'NOT_SET', 'LoadBalancerName': 'pierone-*'}, 'Latency', 'Average')
    print(json.dumps(data))
