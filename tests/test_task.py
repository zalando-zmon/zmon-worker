
from zmon_worker_monitor.zmon_worker.tasks.main import MainTask

from mock import MagicMock

def test_check(monkeypatch):
    MainTask.configure({})
    task = MainTask()
    monkeypatch.setattr(task, '_get_check_result', MagicMock())
    monkeypatch.setattr(task, '_store_check_result', MagicMock())
    monkeypatch.setattr(task, 'send_metrics', MagicMock())
    req = {'check_id': 123, 'entity': {'id': 'myent'}}
    task.check(req)
