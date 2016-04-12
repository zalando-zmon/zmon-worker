import datetime
import pytest
from zmon_worker_monitor.builtins.plugins.cloudwatch import CloudwatchWrapper

from mock import MagicMock


def test_cloudwatch(monkeypatch):
    client = MagicMock()
    client.list_metrics.return_value = {'Metrics': [{'MetricName': 'Latency',
                                                     'Namespace': 'ELB',
                                                     'Dimensions': [
                                                         {'Name': 'LoadBalancerName', 'Value': 'pierone-1'}]},
                                                    {'MetricName': 'Latency',
                                                     'Namespace': 'ELB',
                                                     'Dimensions': [
                                                         {'Name': 'AvailabilityZone', 'Value': 'az-1'},
                                                         {'Name': 'LoadBalancerName', 'Value': 'pierone-1'}]},
                                                    {'MetricName': 'Latency',
                                                     'Namespace': 'ELB',
                                                     'Dimensions': [
                                                         {'Name': 'LoadBalancerName', 'Value': 'otherapp-1'}]}
                                                    ]}
    client.get_metric_statistics.return_value = {
        'Datapoints': [{'Timestamp': datetime.datetime.now(), 'Average': 100.25}]}
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    cloudwatch = CloudwatchWrapper()
    elb_data = cloudwatch.query({'AvailabilityZone': 'NOT_SET', 'LoadBalancerName': 'pierone-*'}, 'Latency', 'Average',
                                namespace='ELB')
    assert {'Latency': 100.25, 'dimensions': {'LoadBalancerName': {'pierone-1': 100.25}}} == elb_data


def test_cloudwatch_query_one_bad_period(monkeypatch):
    client = MagicMock()
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    cloudwatch = CloudwatchWrapper()
    with pytest.raises(ValueError):
        cloudwatch.query_one({'LoadBalancerName': 'pierone-1'}, 'Latency', 'Average', 'AWS/ELB', period=90)


def test_cloudwatch_query_one(monkeypatch):
    client = MagicMock()
    client.get_metric_statistics.return_value = {
        'Datapoints': [
            {'Timestamp': 99, 'Average': 111.25},
            {'Timestamp': 11, 'Average': 100.25}
        ]}
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    cloudwatch = CloudwatchWrapper()
    start = datetime.datetime.now()
    end = start # makes no sense, but works for our test
    elb_data = cloudwatch.query_one({'LoadBalancerName': 'pierone-1'}, 'Latency', 'Average', 'AWS/ELB', start=start, end=end)
    assert 111.25 == elb_data
    assert not client.list_metrics.called
    client.get_metric_statistics.assert_called_with(Namespace='AWS/ELB', MetricName='Latency',
            Dimensions=[{'Name': 'LoadBalancerName', 'Value': 'pierone-1'}],
            StartTime=start,
            EndTime=end,
            Period=60,
            Statistics=['Average'])


def test_cloudwatch_query_one_no_result(monkeypatch):
    client = MagicMock()
    client.get_metric_statistics.return_value = {'Datapoints': []}
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    cloudwatch = CloudwatchWrapper()
    elb_data = cloudwatch.query_one({'AvailabilityZone': 'NOT_SET', 'LoadBalancerName': 'pierone-1'}, 'Latency', 'Average', 'AWS/ELB')
    assert elb_data is None
    assert not client.list_metrics.called
