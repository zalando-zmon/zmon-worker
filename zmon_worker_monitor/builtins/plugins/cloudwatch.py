#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import collections
import datetime
import fnmatch
import logging

from zmon_worker_monitor.zmon_worker.errors import CheckError
from zmon_worker_monitor.builtins.plugins.aws_common import get_instance_identity_document
from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

STATE_OK = 'OK'
STATE_ALARM = 'ALARM'
STATE_DATA = 'INSUFFICIENT_DATA'

MAX_ALARM_RECORDS = 50

logging.getLogger('botocore').setLevel(logging.WARN)

logger = logging.getLogger('zmon-worker.cloudwatch')


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


def matches(dimensions, filters):
    for key, pattern in filters.items():
        if not fnmatch.fnmatch(''.join(dimensions.get(key, '')), pattern):
            return False
    return True


class CloudwatchWrapper(object):
    def __init__(self, region=None, assume_role_arn=None):
        if not region:
            region = get_instance_identity_document()['region']
        self.__client = boto3.client('cloudwatch', region_name=region)

        if assume_role_arn:
            sts = boto3.client('sts', region_name=region)
            resp = sts.assume_role(RoleArn=assume_role_arn, RoleSessionName='zmon-woker-session')
            session = boto3.Session(aws_access_key_id=resp['Credentials']['AccessKeyId'],
                                    aws_secret_access_key=resp['Credentials']['SecretAccessKey'],
                                    aws_session_token=resp['Credentials']['SessionToken'])
            self.__client = session.client('cloudwatch', region_name=region)
            logger.debug('Cloudwatch wrapper assumed role: {}'.format(assume_role_arn))

    def query_one(self, dimensions, metric_name, statistics, namespace, period=60, minutes=5, start=None, end=None,
                  extended_statistics=None):
        '''Query single metric statistic and return scalar value (float), all parameters need to be known in advance'''

        params = {}

        if period < 60 or period % 60 != 0:
            raise ValueError('Period must be greater than and a multiple of 60')

        if isinstance(extended_statistics, basestring):
            extended_statistics = [extended_statistics]

        if isinstance(statistics, basestring):
            statistics = [statistics]

        # special case to gather all types at once
        if statistics is None and extended_statistics is None:
            statistics = ['Sum', 'Average', 'Maximum', 'SampleCount', 'Minimum']
            params['Statistics'] = statistics
        elif statistics is None and extended_statistics is not None:
            params['ExtendedStatistics'] = extended_statistics
        elif statistics is not None and extended_statistics is None:
            params['Statistics'] = statistics
        elif statistics is not None and extended_statistics is not None:
            params['Statistics'] = statistics
            params['ExtendedStatistics'] = extended_statistics

        if isinstance(dimensions, dict):
            # transform Python dict to stupid AWS list structure
            # see http://boto3.readthedocs.org/en/latest/reference/services/cloudwatch.html#CloudWatch.Client.get_metric_statistics  # noqa
            dimensions = list({'Name': k, 'Value': v} for k, v in dimensions.items())

        end = end or datetime.datetime.utcnow()
        start = start or (end - datetime.timedelta(minutes=minutes))

        params['Namespace'] = namespace
        params['MetricName'] = metric_name
        params['Dimensions'] = dimensions
        params['StartTime'] = start
        params['EndTime'] = end
        params['Period'] = period

        response = self.__client.get_metric_statistics(**params)
        data_points = sorted(response['Datapoints'], key=lambda x: x["Timestamp"])

        result = {}
        if not data_points:
            return None

        if extended_statistics is None and len(statistics) == 1:
            result = data_points[-1][statistics[0]]
        elif statistics is not None:
            result.update({s: v for s, v in data_points[-1].items() if s in statistics})

        if statistics is None and len(extended_statistics) == 1:
            result = data_points[-1].get('ExtendedStatistics', {}).get(extended_statistics[0])
        elif extended_statistics is not None:
            result.update({
                s: v for s, v in data_points[-1].get('ExtendedStatistics', {}).items() if s in extended_statistics
            })

        return result

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

    def alarms(self, alarm_names=None, alarm_name_prefix=None, state_value=STATE_ALARM, action_prefix=None,
               max_records=50):
        """
        Retrieve cloudwatch alarms.

        :param alarm_names: List of alarm names.
        :type alarm_names: list

        :param alarm_name_prfix: Prefix of alarms. Cannot be specified if ``alarm_names`` is specified.
        :type alarm_name_prfix: str

        :param state_value: State value used in alarm filtering. Available values are STATE_OK, STATE_ALARM(default) and STATE_DATA.
        :type state_value: str

        :param action_prefix: Action prefix.
        :type action_prefix: str

        :param max_records: Maximum records to be returned. Default is 50.
        :type max_records: int

        :return: List of MetricAlarms.
        :rtype: list
        """  # noqa
        if alarm_names and alarm_name_prefix:
            raise CheckError('"alarm_name_prefix" cannot be sprecified if "alarm_names" is specified!')

        kwargs = dict(MaxRecords=max_records)
        if state_value:
            kwargs.update({'StateValue': state_value})

        if alarm_names:
            alarm_names = [alarm_names] if isinstance(alarm_names, basestring) else alarm_names
            kwargs['AlarmNames'] = alarm_names
        elif alarm_name_prefix:
            kwargs['AlarmNamePrefix'] = alarm_name_prefix

        if action_prefix:
            kwargs['ActionPrefix'] = action_prefix

        return self.__client.describe_alarms(**kwargs)['MetricAlarms']
