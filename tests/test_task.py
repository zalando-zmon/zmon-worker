from zmon_worker_monitor.zmon_worker.tasks.main import check_filter_metric, check_filter_metrics

def test_timer_filter():
    timers = {"a": {"p95": 1, "p98": 2, "rate": 3, "mRate": 4},
              "b": {"p95": 11, "p98": 12, "rate": 13, "mRate": 14}}
    filtered = check_filter_metrics(timers, ["p95", "rate"])

    assert "p98" not in filtered["a"]
    assert "p95" in filtered["a"]
    assert "rate" in filtered["b"]
    assert "mRate" not in filtered["b"]
