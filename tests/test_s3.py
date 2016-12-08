from botocore.exceptions import ClientError

from zmon_worker_monitor.builtins.plugins.s3 import S3Wrapper

from mock import MagicMock, DEFAULT, ANY


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
