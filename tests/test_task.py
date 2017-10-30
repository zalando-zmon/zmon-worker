from zmon_worker_monitor.zmon_worker.tasks.main import check_filter_metrics, monotonic

from mock import MagicMock
import pytest
import logging


logger = logging.getLogger(__name__)


def test_timer_filter():
    timers = {"a": {"p95": 1, "p98": 2, "rate": 3, "mRate": 4},
              "b": {"p95": 11, "p98": 12, "rate": 13, "mRate": 14}}
    filtered = check_filter_metrics(timers, ["p95", "rate"])

    assert "p98" not in filtered["a"]
    assert "p95" in filtered["a"]
    assert "rate" in filtered["b"]
    assert "mRate" not in filtered["b"]


@pytest.mark.parametrize('count,increasing,strictly,data,expected',
                         ((10, True, False, [9, 8, 7, 6, 4, 4, 3, 2, 1], True),
                          (10, True, True, [9, 8, 7, 6, 5, 4, 3, 2, 1], True),
                          (10, False, False, [1, 2, 3, 4, 4, 6, 7, 8, 9], True),
                          (10, False, True, [1, 2, 3, 4, 5, 6, 7, 8, 9], True),
                          (10, True, True, [9, 8, 7, 6, 5, 5, 3, 2, 1], False)))
def test_monotonic(monkeypatch, count, increasing, strictly, data, expected):
    alert_series = MagicMock()
    alert_series.return_value = data
    monkeypatch.setattr('zmon_worker_monitor.zmon_worker.tasks.main.get_results_user', alert_series)
    ret = monotonic(count, increasing=increasing, strictly=strictly)
    assert ret is expected
