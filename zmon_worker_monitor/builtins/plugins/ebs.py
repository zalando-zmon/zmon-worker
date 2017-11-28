# -*- coding: utf-8 -*-

import boto3
import logging

from zmon_worker_monitor.builtins.plugins.aws_common import get_instance_identity_document
from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logging.getLogger('botocore').setLevel(logging.WARN)


class EBSWrapperFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(EBSWrapperFactory, self).__init__()

    def configure(self, conf):
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(EBSWrapper, region=factory_ctx.get('entity').get('region', None))


class EBSWrapper(object):

    def __init__(self, region=None):
        if not region:
            region = get_instance_identity_document()['region']
        self.__client = boto3.client('ec2', region_name=region)

    def list_snapshots(self, account_id=None, max_items=100):
        """
        List EBS snapshots in the specified account.
        :param account_id: AWS account number (as a string)
        :param max_items: the maximum number of EBS snapshots to list
        :return: an EBSSnapshotsList object
        """
        if not account_id:
            account_id = get_instance_identity_document()['accountId']
        paginator = self.__client.get_paginator('describe_snapshots')
        response = paginator.paginate(OwnerIds=[account_id], PaginationConfig={'MaxItems': max_items}) \
                            .build_full_result()

        return EBSSnapshotsList(response)


class EBSSnapshotsList(object):

    def __init__(self, response):
        self.__response = response
        self.__has_contents = 'Snapshots' in self.__response

    def items(self):
        """
        The list of Snapshots found
        :return: a list of dicts
            [{'id': 'string', 'start_time': datetime(2015, 1, 15, 14, 34, 56), 'size': 123}, ...]
        """
        if self.__has_contents:
            return [dict(zip(['id', 'description', 'size', 'start_time', 'state'],
                             [item['SnapshotId'], item['Description'], item['VolumeSize'],
                              item['StartTime'], item['State']]))
                    for item in self.__response['Snapshots']]
        else:
            return []
