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
    end = start  # makes no sense, but works for our test
    elb_data = cloudwatch.query_one({'LoadBalancerName': 'pierone-1'}, 'Latency', 'Average', 'AWS/ELB', start=start,
                                    end=end)
    assert 111.25 == elb_data
    assert not client.list_metrics.called
    client.get_metric_statistics.assert_called_with(Namespace='AWS/ELB', MetricName='Latency',
                                                    Dimensions=[{'Name': 'LoadBalancerName', 'Value': 'pierone-1'}],
                                                    StartTime=start,
                                                    EndTime=end,
                                                    Period=60,
                                                    Statistics=['Average'])


def test_cloudwatch_query_one_multiple_statistics(monkeypatch):
    client = MagicMock()
    client.get_metric_statistics.return_value = {
        'Datapoints': [
            {'Timestamp': 99, 'Average': 111.25, 'Minimum': 1},
            {'Timestamp': 11, 'Average': 100.25}
        ]}
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    cloudwatch = CloudwatchWrapper()
    start = datetime.datetime.now()
    end = start  # makes no sense, but works for our test
    elb_data = cloudwatch.query_one({'LoadBalancerName': 'pierone-1'}, 'Latency', None, 'AWS/ELB', start=start, end=end)
    assert {'Average': 111.25, 'Minimum': 1} == elb_data
    assert not client.list_metrics.called
    client.get_metric_statistics.assert_called_with(Namespace='AWS/ELB', MetricName='Latency',
                                                    Dimensions=[{'Name': 'LoadBalancerName', 'Value': 'pierone-1'}],
                                                    StartTime=start,
                                                    EndTime=end,
                                                    Period=60,
                                                    Statistics=['Sum', 'Average', 'Maximum', 'SampleCount', 'Minimum'])


def test_cloudwatch_query_one_no_result(monkeypatch):
    client = MagicMock()
    client.get_metric_statistics.return_value = {'Datapoints': []}
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    cloudwatch = CloudwatchWrapper()
    elb_data = cloudwatch.query_one({'AvailabilityZone': 'NOT_SET', 'LoadBalancerName': 'pierone-1'}, 'Latency',
                                    'Average', 'AWS/ELB')
    assert elb_data is None
    assert not client.list_metrics.called


def test_cloudwatch_paging(monkeypatch):
    client = MagicMock()

    call_count = {'list_metrics': 0, 'get_metric_statistics': 0}

    def my_list_metrics(*args, **kwargs):
        call_count['list_metrics'] += 1
        if 'NextToken' in kwargs:
            return {'Metrics': [{'MetricName': 'NetworkOut',
                                 'Namespace': 'AWS/EC2',
                                 'Dimensions': [
                                     {'Name': 'AutoScalingGroupName', 'Value': 'tailor-1'}]}]}
        else:
            return {'Metrics': [{'MetricName': 'NetworkOut',
                                 'Namespace': 'AWS/EC2',
                                 'Dimensions': [
                                     {'Value': 'i-123456789', 'Name': 'InstanceId'}]},
                                {'MetricName': 'NetworkOut',
                                 'Namespace': 'AWS/EC2',
                                 'Dimensions': [
                                     {'Value': 'i-987654321', 'Name': 'InstanceId'}]},
                                {'MetricName': 'NetworkOut',
                                 'Namespace': 'AWS/EC2',
                                 'Dimensions': [
                                     {'Value': 'other-asg-1VSSQK1FPN2A', 'Name': 'AutoScalingGroupName'}]}
                                ],
                    'NextToken': 'dummyToken'}

    def my_get_metric_statistics(*args, **kwargs):
        call_count['get_metric_statistics'] += 1
        if kwargs['Dimensions'][0]['Name'] == 'AutoScalingGroupName' and kwargs['Dimensions'][0]['Value'] == 'tailor-1':
            return {'Datapoints': [{'Timestamp': datetime.datetime.now(), 'Average': 42.0}]}

    client.list_metrics = my_list_metrics
    client.get_metric_statistics = my_get_metric_statistics

    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    cloudwatch = CloudwatchWrapper(region='dummy-region')
    data = cloudwatch.query({'AvailabilityZone': 'NOT_SET', 'AutoScalingGroupName': 'tailor-*'},
                            'NetworkOut', 'Average',
                            namespace='AWS/EC2')

    assert call_count['list_metrics'] is 2
    assert call_count['get_metric_statistics'] is 1

    assert {'NetworkOut': 42.0, 'dimensions': {'AutoScalingGroupName': {'tailor-1': 42.0}}} == data
