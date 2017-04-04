from zmon_worker_monitor.builtins.plugins.datapipeline import DataPipelineWrapper

from mock import MagicMock


def test_get_details_one_pipeline(monkeypatch):
    client = MagicMock()
    client.describe_pipelines.return_value = {'pipelineDescriptionList': [{'fields': [
      {u'key': u'@cancelActive',
       u'stringValue': u'true'},
      {'key': '@id',
       'stringValue': 'pipeline_1'},
      {'key': '@accountId',
       'stringValue': '123456789'}],
      'name': 'pipeline_1',
      'pipelineId': 'pipeline_1',
      'tags': [{'key': 'DataPipelineName',
                'value': 'pipeline_1'},
               {'key': 'DataPipelineId',
                'value': 'pipeline_1'}]}]}

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.get_details('pipeline_1')
    assert elb_data == {'pipeline_1': {'@accountId': '123456789',
                                       '@cancelActive': 'true',
                                       '@id': 'pipeline_1'}}

    client.describe_pipelines.assert_called_with(pipelineIds=['pipeline_1'])


def test_get_details_multiple_pipelines(monkeypatch):
    client = MagicMock()
    client.describe_pipelines.return_value = {'pipelineDescriptionList': [{'fields': [
      {'key': '@cancelActive',
       'stringValue': 'true'},
      {'key': '@id',
       'stringValue': 'pipeline_1'},
      {'key': '@accountId',
       'stringValue': '123456789'}],
      'name': 'pipeline_1',
      'pipelineId': 'pipeline_1',
      'tags': [{'key': 'DataPipelineName',
                'value': 'pipeline_1'},
               {'key': 'DataPipelineId',
                'value': 'pipeline_1'}]},
      {'fields': [{'key': '@sphere',
                   'stringValue': 'PIPELINE'},
                  {'key': '@healthStatus',
                   'stringValue': 'HEALTHY'},
                  {'key': '@id',
                   'stringValue': 'pipeline_2'},
                  {'key': '@pipelineState',
                   'stringValue': 'SCHEDULED'}],
       'name': 'pipeline_2',
       'pipelineId': 'pipeline_2',
       'tags': [{'key': 'ClusterName',
                 'value': 'pipeline_2'},
                {'key': 'DataPipelineId',
                 'value': 'pipeline_2'}]}]}

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.get_details(['pipeline_1', 'pipeline_2'])
    assert elb_data == {'pipeline_2': {'@healthStatus': 'HEALTHY',
                                       '@id': 'pipeline_2',
                                       '@pipelineState': 'SCHEDULED',
                                       '@sphere': 'PIPELINE'},
                        'pipeline_1': {'@accountId': '123456789',
                                       '@cancelActive': 'true',
                                       '@id': 'pipeline_1'}}

    client.describe_pipelines.assert_called_with(pipelineIds=['pipeline_1', 'pipeline_2'])
