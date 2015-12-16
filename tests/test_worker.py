
import time
from zmon_worker_monitor.web import main

def test_check(monkeypatch):
    monkeypatch.setattr('zmon_worker_monitor.redis_context_manager.RedisConnHandler.get_conn', lambda x: None)

    proc = main(['--no-rpc'])
    # TODO: test check
    time.sleep(0.1)
    proc.proc_control.terminate_all_processes()



