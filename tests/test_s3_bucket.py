from zmon_worker_monitor.builtins.plugins.s3_bucket import S3BucketWrapper

from mock import MagicMock, DEFAULT, ANY

def test_raw_object_should_be_found_and_returned(monkeypatch):
    client = MagicMock()
    def writer_side_effect(*args, **kwargs):
        args[2].write('some random content')
        return DEFAULT 
    client.download_fileobj.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_bucket = S3BucketWrapper()
    
    raw_object = s3_bucket.get_raw_object('bucket', 'key')
    
    client.download_fileobj.assert_called_with('bucket', 'key', ANY)
    assert raw_object is not None
    assert 'some random content' == raw_object
    
def test_json_object_should_be_found_and_returned(monkeypatch):
    client = MagicMock()
    def writer_side_effect(*args, **kwargs):
        args[2].write('{"some": "random", "content": "is here"}')
        return DEFAULT 
    client.download_fileobj.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    s3_bucket = S3BucketWrapper()
    
    json_object = s3_bucket.get_json_object('bucket', 'key')
    
    client.download_fileobj.assert_called_with('bucket', 'key', ANY)
    assert json_object is not None
    assert {'content': 'is here', 'some': 'random'} == json_object    
    