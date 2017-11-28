from botocore.exceptions import ClientError

from zmon_worker_monitor.builtins.plugins.s3 import S3Wrapper

from mock import MagicMock, DEFAULT, ANY

import pytest

from datetime import datetime


def test_metadata_on_existing_object(monkeypatch):

    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        return {'ContentLength': 75}
    client.head_object.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    meta_data = s3_wrapper.get_object_metadata('bucket', 'key')

    assert meta_data is not None
    assert meta_data.exists() is True
    assert meta_data.size() is 75


def test_metadata_on_non_existent_object(monkeypatch):

    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        raise ClientError({'Error': {}}, 'some operation')
    client.head_object.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    meta_data = s3_wrapper.get_object_metadata('bucket', 'key')

    assert meta_data is not None
    assert meta_data.exists() is False
    assert meta_data.size() is -1


def test_object_should_not_be_found_and_text_not_returned(monkeypatch):
    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        raise ClientError({'Error': {}}, 'some operation')
    client.download_fileobj.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    s3_object = s3_wrapper.get_object('bucket', 'key')
    raw_object = s3_object.text()

    client.download_fileobj.assert_called_with('bucket', 'key', ANY)
    assert raw_object is None
    assert s3_object.exists() is False
    assert s3_object.size() is -1


def test_object_should_be_found_and_text_returned(monkeypatch):
    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        args[2].write('some random content')
        return DEFAULT
    client.download_fileobj.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    s3_object = s3_wrapper.get_object('bucket', 'key')
    raw_object = s3_object.text()

    client.download_fileobj.assert_called_with('bucket', 'key', ANY)
    assert raw_object is not None
    assert 'some random content' == raw_object
    assert s3_object.exists() is True
    assert s3_object.size() is 19


def test_object_should_be_found_and_json_returned(monkeypatch):
    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        args[2].write('{"some": "random", "content": "is here"}')
        return DEFAULT
    client.download_fileobj.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    s3_object = s3_wrapper.get_object('bucket', 'key')
    json_object = s3_object.json()

    client.download_fileobj.assert_called_with('bucket', 'key', ANY)
    assert json_object is not None
    assert {'content': 'is here', 'some': 'random'} == json_object
    assert s3_object.exists() is True
    assert s3_object.size() is 40


def test_listing_on_existing_prefix(monkeypatch):

    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        return {'Contents': [{'Key': 'some_file', 'Size': 123, 'LastModified': datetime(2015, 1, 15, 14, 34, 56)}]}
    client.get_paginator.return_value.paginate.return_value.build_full_result.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    file_list = s3_wrapper.list_bucket('bucket', 'prefix')

    assert file_list is not None
    assert len(file_list.files()) is 1
    assert file_list.files()[0]['file_name'] is 'some_file'
    assert file_list.files()[0]['size'] is 123
    assert file_list.files()[0]['last_modified'] == datetime(2015, 1, 15, 14, 34, 56)


def test_listing_on_prefix_that_has_no_objects(monkeypatch):
    """
    Can be either because the prefix does not exist or there are no objects under it
    """

    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        return {}
    client.get_paginator.return_value.paginate.return_value.build_full_result.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    file_list = s3_wrapper.list_bucket('bucket', 'prefix')

    assert file_list is not None
    assert len(file_list.files()) is 0


def test_listing_bubbles_client_error_up(monkeypatch):
    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        raise ClientError({'Error': {'Code': 403, 'Message': 'Access denied'}}, 'information')
    client.get_paginator.return_value.paginate.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    with pytest.raises(ClientError) as ex:
        s3_wrapper.list_bucket('bucket', 'prefix').files()

    assert 'Access denied' == ex.value.response['Error']['Message']


def test_bucket_exists(monkeypatch):
    client = MagicMock()
    client.head_bucket.return_value = {}
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    assert s3_wrapper.bucket_exists('foo') is True


def test_bucket_exists_not(monkeypatch):
    client = MagicMock()
    client.head_bucket.side_effect = Exception("no such bucket")
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_wrapper = S3Wrapper()

    assert s3_wrapper.bucket_exists('foo') is False
    client.head_bucket.assert_called_with(Bucket='foo')
