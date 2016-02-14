import datetime
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
