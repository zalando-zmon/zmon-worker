#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json
import logging
import requests
import cStringIO

from botocore.exceptions import ClientError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logging.getLogger('botocore').setLevel(logging.WARN)


class S3BucketWrapper(IFunctionFactoryPlugin):
    def __init__(self):
        super(S3BucketWrapper, self).__init__()

    def configure(self, conf):
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(S3Bucket, region=factory_ctx.get('entity').get('region', None))


def get_region():
    r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=3)
    return r.json()['region']


class S3Bucket(object):
    def __init__(self, region=None):
        if not region:
            region = get_region()
        self.__client = boto3.client('s3', region_name=region)

    def get_object_metadata(self, bucket_name, key):
        try:
            response = self.__client.head_object(Bucket=bucket_name, Key=key)
            return S3ObjectMetadata(response)
        except ClientError:
            return S3ObjectMetadata({})

    def get_object(self, bucket_name, key):
        data = cStringIO.StringIO()
        try:
            self.__client.download_fileobj(bucket_name, key, data)
            result = data.getvalue()
            return S3Object(result)
        except ClientError:
            return S3Object(None)
        finally:
            data.close()


class S3Object(object):

    def __init__(self, key_value):
        self.__key_value = key_value

    def json(self):
        if self.exists():
            return json.loads(self.__key_value)
        else:
            return None

    def text(self):
        return self.__key_value

    def exists(self):
        return self.__key_value is not None

    def size(self):
        if self.exists():
            return len(self.__key_value)
        else:
            return -1


class S3ObjectMetadata(object):

    def __init__(self, response):
        self.__response = response

    def exists(self):
        return len(self.__response) > 0

    def size(self):
        if self.exists():
            return self.__response['ContentLength']
        else:
            return -1
