from datetime import datetime

import pytest
import pytz
from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.time_ import TimeWrapper

BERLIN_TZ = pytz.timezone('Europe/Berlin')


def test_now_isoformat(monkeypatch):
    dt = MagicMock()
    dt.now.return_value.isoformat.return_value = 'NOW'
    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.time_.datetime', dt)
    assert 'NOW' == TimeWrapper().isoformat()


@pytest.mark.parametrize('inp,out', [
    ({'spec': 1467284283}, datetime.fromtimestamp(1467284283)),
    ({'spec': 1467284283.145063}, datetime.fromtimestamp(1467284283.145063)),
    ({'spec': 1467284283, 'utc': True}, datetime.utcfromtimestamp(1467284283)),
    ({'spec': 14672842, 'tz_name': 'Europe/Berlin'}, datetime.fromtimestamp(14672842, BERLIN_TZ))
])
def test_time_epoch(monkeypatch, inp, out):
    tw = TimeWrapper(**inp)
    assert out == tw.time


def test_time_sub(monkeypatch):
    tw1 = TimeWrapper('2016-01-01 00:00:00')
    tw2 = TimeWrapper('2016-01-01 00:00:59')

    assert 59 == tw2 - tw1


def test_raise_error_if_utc_and_tz_given():
    with pytest.raises(ValueError):
        TimeWrapper(0, True, 'Europe/Berlin')


def test_init_timezone():
    berlin_now = TimeWrapper(tz_name='Europe/Berlin').time
    assert (berlin_now - datetime.now(BERLIN_TZ)).total_seconds() < 1


def test_astimezone():
    tw = TimeWrapper('2016-01-01 01:00:00', utc=True)
    assert tw.astimezone('Europe/Berlin').isoformat() == '2016-01-01 02:00:00+01:00'
