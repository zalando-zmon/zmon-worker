from zmon_worker_monitor.zmon_worker.tasks.main import urlparse
import urllib3.util


def test_urlparse():
    assert urlparse(5.0) is None
    assert urlparse("http://localhost:8080/foo") == urllib3.util.Url(scheme='http',
                                                                     auth=None, host='localhost',
                                                                     port=8080, path='/foo', query=None,
                                                                     fragment=None)
