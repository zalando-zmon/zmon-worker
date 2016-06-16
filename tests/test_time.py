from mock import MagicMock
from zmon_worker_monitor.builtins.plugins.time_ import TimeWrapper


def test_now_isoformat(monkeypatch):
    dt = MagicMock()
    dt.now.return_value.isoformat.return_value = 'NOW'
    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.time_.datetime', dt)
    assert 'NOW' == TimeWrapper().isoformat()
