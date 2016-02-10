
from zmon_worker_monitor.zmon_worker.tasks.notacelery_task import NotaZmonTask

from mock import MagicMock

def test_check(monkeypatch):
    NotaZmonTask.configure({})
    task = NotaZmonTask()
    monkeypatch.setattr(task, '_get_check_result', MagicMock())
    monkeypatch.setattr(task, '_store_check_result', MagicMock())
    monkeypatch.setattr(task, 'send_metrics', MagicMock())
    req = {'check_id': 123, 'entity': {'id': 'myent'}}
    task.check(req)
