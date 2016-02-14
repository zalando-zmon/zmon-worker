import pytest
from mock import MagicMock
from zmon_worker_monitor.builtins.plugins.http import HttpWrapper
from zmon_worker_monitor.zmon_worker.errors import HttpError

def get_dropwizard_metrics():
    yield {
    "version": "3.0.0",
    "gauges": {},
    "counters": {},
    "histograms": {},
    "meters": {},
    "timers": {
        "zmon.response.200.GET.pets": {
        "count": 1917,
        "max": 1125,
        "mean": 386.16423467091,
        "min": 295,
        "p50": 383,
        "p75": 383,
        "p95": 383,
        "p98": 460,
        "p99": 460,
        "p999": 460,
        "stddev": 15.285876814113,
        "m15_rate": 0.0027894332885165,
        "m1_rate": 3.7570008143941E-5,
        "m5_rate": 0.0016195023085788,
        "mean_rate": 0.0031567415804972,
        "duration_units": "milliseconds",
        "rate_units": "calls/second"
        },
        "zmon.response.200.GET.pets.{pet_id}": {
        "count": 392373,
        "max": 627,
        "mean": 219.38202968217,
        "min": 163,
        "p50": 218,
        "p75": 224,
        "p95": 249,
        "p98": 265,
        "p99": 425,
        "p999": 425,
        "stddev": 30.77609293132,
        "m15_rate": 0.69320705677888,
        "m1_rate": 0.67804789230544,
        "m5_rate": 0.71816217263666,
        "mean_rate": 0.64605322610366,
        "duration_units": "milliseconds",
        "rate_units": "calls/second"
        }
    }
    }
    # expected
    yield {'pets': {'GET': {'200': {'75th': 383,
                                     '99th': 460,
                                     'count': 1917,
                                     'mRate': 3.7570008143941e-05, 'median': 383, 'min': 295, 'max': 1125,
                                     'mean': 386.16423467091}}},
           'pets.{pet_id}': {'GET': {'200': {'75th': 224,
                                                '99th': 425,
                                                'count': 392373,
                                                'mRate': 0.67804789230544,
                                                'max': 627,
                                                'median': 218,
                                                'min': 163,
                                                'mean': 219.38202968217}}}}



def get_test_data():
    return [({}, {}),
            tuple(get_dropwizard_metrics())]

def test_http(monkeypatch):
    resp = MagicMock()
    resp.status_code = 200
    resp.text = '"foo"'
    resp.content = resp.text
    resp.json.return_value = 'foo'
    get = MagicMock()
    get.return_value = resp
    monkeypatch.setattr('requests.get', get)
    http = HttpWrapper('http://example.org')
    assert 200 == http.code()
    assert '"foo"' == http.text()
    assert 'foo' == http.json()
    assert 5 == http.content_size()


def test_http_actuator_metrics_invalid(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = 'foo'
    get = MagicMock()
    get.return_value = resp
    monkeypatch.setattr('requests.get', get)
    http = HttpWrapper('http://example.org')
    with pytest.raises(HttpError) as ex:
        http.actuator_metrics()
    assert 'Invalid actuator metrics: response must be a JSON object' == ex.value.message


@pytest.mark.parametrize('metrics_response,expected', get_test_data())
def test_http_actuator_metrics_valid(monkeypatch, metrics_response, expected):
    resp = MagicMock()
    resp.json.return_value = metrics_response
    get = MagicMock()
    get.return_value = resp
    monkeypatch.setattr('requests.get', get)
    http = HttpWrapper('http://example.org')
    assert expected == http.actuator_metrics()
