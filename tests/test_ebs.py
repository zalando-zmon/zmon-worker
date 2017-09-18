from zmon_worker_monitor.builtins.plugins.ebs import EBSWrapper

from mock import MagicMock

from datetime import datetime


def test_listing_snapshots(monkeypatch):

    client = MagicMock()

    def writer_side_effect(*args, **kwargs):
        return {'Snapshots': [{'SnapshotId': 'snap-12345',
                               'Description': 'dummy',
                               'VolumeSize': 123,
                               'StartTime': datetime(2015, 1, 15, 14, 34, 56),
                               'State': 'completed'}]}
    client.get_paginator.return_value.paginate.return_value.build_full_result.side_effect = writer_side_effect
    get = MagicMock()
    get.return_value.json.return_value = {'region': 'eu-central-1', 'accountId': '1234567890'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    ebs_wrapper = EBSWrapper()

    snap_list = ebs_wrapper.list_snapshots()

    assert snap_list is not None
    assert snap_list.items() == [{
        'id': 'snap-12345',
        'description': 'dummy',
        'size': 123,
        'start_time': datetime(2015, 1, 15, 14, 34, 56),
        'state': 'completed'
    }]
