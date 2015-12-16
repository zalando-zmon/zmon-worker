#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import collections
import datetime
import fnmatch
import json
import sys
import logging
import requests
import sys

logging.getLogger('botocore').setLevel(logging.WARN)

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

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
        self.client = boto3.client('cloudwatch', region_name=region)

    def query(self, dimensions, metric_name, statistics='Sum', namespace=None, unit=None, period=60, minutes=5):
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
        metrics = self.client.list_metrics(**args)
        metrics = metrics['Metrics']
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
            response = self.client.get_metric_statistics(Namespace=metric['Namespace'], MetricName=metric['MetricName'], Dimensions=metric['Dimensions'],
                                                         StartTime=start, EndTime=end, Period=period, Statistics=[statistics])
            data_points = response['Datapoints']
            if data_points:
                for [dim_name, dim_val] in metric_dimensions.items():
                    if not dim_name in data['dimensions']:
                        data['dimensions'][dim_name] = collections.defaultdict(int)
                    data['dimensions'][dim_name][dim_val] += data_points[-1][statistics]
                data[metric['MetricName']] += data_points[-1][statistics]
        return data


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    cloudwatch = CloudwatchWrapper(sys.argv[1])
    print "ELB result (eu-west-1 only):"
    elb_data = cloudwatch.query({'AvailabilityZone': 'NOT_SET', 'LoadBalancerName': 'pierone-*'}, 'Latency', 'Average')
    print(json.dumps(elb_data))
    print "Billing result (us-east-1 only):"
    billing_data = cloudwatch.query({'Currency': 'USD'}, 'EstimatedCharges', 'Maximum', 'AWS/Billing', None, 3600, 60*4)
    print(json.dumps(billing_data))
