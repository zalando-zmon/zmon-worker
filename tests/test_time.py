from datetime import datetime

import pytest
from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.time_ import TimeWrapper


def test_now_isoformat(monkeypatch):
    dt = MagicMock()
    dt.now.return_value.isoformat.return_value = 'NOW'
    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.time_.datetime', dt)
    assert 'NOW' == TimeWrapper().isoformat()


@pytest.mark.parametrize('inp,out', [
    ({'epoch': 1467284283}, datetime(2016, 6, 30, 12, 58, 3)),
    ({'epoch': 1467284283.145063}, datetime(2016, 6, 30, 12, 58, 3, 145063)),
    ({'epoch': 1467284283, 'utc': True}, datetime(2016, 6, 30, 10, 58, 3))
])
def test_time_epoch(monkeypatch, inp, out):
    tw = TimeWrapper(**inp)
    assert out == tw.time


def test_time_sub(monkeypatch):
    tw1 = TimeWrapper('2016-01-01 00:00:00')
    tw2 = TimeWrapper('2016-01-01 00:00:59')

    assert 59 == tw2 - tw1
