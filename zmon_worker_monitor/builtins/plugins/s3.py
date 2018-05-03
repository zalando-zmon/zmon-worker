#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import json
import logging
import cStringIO

from botocore.exceptions import ClientError

from zmon_worker_monitor.builtins.plugins.aws_common import get_instance_identity_document
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
        return propartial(S3Wrapper, region=factory_ctx.get('entity').get('region', None))


class S3Wrapper(object):
    def __init__(self, region=None):
        if not region:
            region = get_instance_identity_document()['region']
        self.__client = boto3.client('s3', region_name=region)

    def get_object_metadata(self, bucket_name, key):
        """
        Get metadata on the object in the given bucket and accessed with the given key.
        The metadata is retieved without fetching the object so it can be safely used with large
        S3 objects
        :param bucket_name: the name of the S3 Bucket
        :param key: the key that identifies the S3 Object within the S3 Bucket
        :return: an S3ObjectMetadata object
        """
        try:
            response = self.__client.head_object(Bucket=bucket_name, Key=key)
            return S3ObjectMetadata(response)
        except ClientError:
            return S3ObjectMetadata({})

    def get_object(self, bucket_name, key):
        """
        Get the object in the given bucket and accessed with the given key.
        The S3 Object is read into memory and the data within can be accessed as a string or parsed as JSON.
        :param bucket_name: the name of the S3 Bucket
        :param key: the key that identifies the S3 Object within the S3 Bucket
        :return: an S3Object object
        """
        data = cStringIO.StringIO()
        try:
            self.__client.download_fileobj(bucket_name, key, data)
            result = data.getvalue()
            return S3Object(result)
        except ClientError:
            return S3Object(None)
        finally:
            data.close()

    def list_bucket(self, bucket_name, prefix, max_items=100, recursive=True):
        """
        List the objects in the bucket under the provided prefix.  Uses a paginator for cases when the number of objects
        exceeds the hard limit of 1000.
        :param bucket_name: the name of the S3 Bucket
        :param prefix: the prefix to search under
        :param max_items: the maximum number of objects to list
        :param recursive: defines if the listing should contain deeply nested keys
        :return: an S3FileList object
        """
        paginator = self.__client.get_paginator('list_objects_v2')
        params = dict(Bucket=bucket_name, Prefix=prefix, PaginationConfig={'MaxItems': max_items})
        if not recursive:
            params['Delimiter'] = '/'
        response = paginator.paginate(**params).build_full_result()

        return S3FileList(response)

    def bucket_exists(self, bucket_name):
        """
        Check if the given bucket exists
        :param bucket_name: the name of the S3 Bucket
        """
        try:
            self.__client.head_bucket(Bucket=bucket_name)
            return True
        except Exception:
            return False


class S3Object(object):

    def __init__(self, key_value):
        self.__key_value = key_value

    def json(self):
        """
        Get the S3 Object data and parse it as JSON
        :return: a dict containing the parsed JSON
        """
        if self.exists():
            return json.loads(self.__key_value)
        else:
            return None

    def text(self):
        """
        Get the S3 Object data (we assume it's text)
        :return: the raw S3 Object data
        """
        return self.__key_value

    def exists(self):
        """
        Does this object exist?
        :return: True if the object exists
        """
        return self.__key_value is not None

    def size(self):
        """
        How large (in bytes) is the object data
        :return: the size in bytes of the object, or -1 if the object does not exist.
        """
        if self.exists():
            return len(self.__key_value)
        else:
            return -1


class S3ObjectMetadata(object):

    def __init__(self, response):
        self.__response = response

    def exists(self):
        """
        Does this object exist?
        :return: True if the object exists
        """
        return len(self.__response) > 0

    def size(self):
        """
        How large (in bytes) is the object data
        :return: the size in bytes of the object, or -1 if the object does not exist.
        """
        if self.exists():
            return self.__response['ContentLength']
        else:
            return -1


class S3FileList(object):

    def __init__(self, response):
        self.__response = response
        self.__has_contents = 'Contents' in self.__response

    def files(self):
        """
        The list of file-like objects found
        :return: a list of dicts
            [{'file_name': 'string', 'last_modified': datetime(2015, 1, 15, 14, 34, 56), 'size': 123}, ...]
        """
        if self.__has_contents:
            return [dict(zip(['file_name', 'last_modified', 'size'],
                             [item['Key'], item['LastModified'], item['Size']]))
                    for item in self.__response['Contents']]
        else:
            return []
