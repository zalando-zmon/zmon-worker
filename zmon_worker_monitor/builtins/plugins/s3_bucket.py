#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json
import logging
import requests
import StringIO

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logging.getLogger('botocore').setLevel(logging.WARN)

class S3BucketWrapperFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(S3BucketWrapperFactory, self).__init__()

    def configure(self, conf):
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(S3BucketWrapper, region=factory_ctx.get('entity').get('region', None))


def get_region():
    r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=3)
    return r.json()['region']

class S3BucketWrapper(object):
    def __init__(self, region=None):
        if not region:
            region = get_region()
        self.__client = boto3.client('s3', region_name=region)

    def get_raw_object(self, bucket_name, key):
        data = StringIO.StringIO()
        self.__client.download_fileobj(bucket_name, key, data)
        result = data.getvalue()
        data.close()
        return result

    def get_json_object(self, bucket_name, key):
        data = StringIO.StringIO()
        self.__client.download_fileobj(bucket_name, key, data)        
        data.seek(0)
        result = json.load(data)
        data.close()
        return result

