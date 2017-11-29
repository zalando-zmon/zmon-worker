import pytest

from zmon_worker_monitor.builtins.plugins.memcached import MemcachedWrapper, ConfigurationError
from mock import MagicMock


class CounterMock(object):

    def wrapper(self):
        return CounterWrapperMock


class CounterWrapperMock(object):

    def __init__(self, name):
        pass

    def per_second(self, value):
        return 0

    def key(self, key):
        return self


def test_memcached_config_error():
    with pytest.raises(ConfigurationError):
        MemcachedWrapper(lambda x: x, None)

    with pytest.raises(ConfigurationError):
        MemcachedWrapper(lambda x: x, '')


def test_memcached_stats(monkeypatch):
        mc = MemcachedWrapper(CounterMock().wrapper(), 'localhost')
        stats = MagicMock()
        stats.return_value = {
            'total_connections': 10,
            'cmd_get': 200,
            'get_hits': 145,
            'get_misses': 55,
            'bytes': 123456,
            'version': 'pymemcache test',
        }

        monkeypatch.setattr('pymemcache.client.base.Client.stats', stats)
        ret = mc.stats(extra_keys=['version', 'uptime'])
        assert 'connections_per_sec' in ret
        assert 'cmd_get_per_sec' in ret
        assert 'bytes_per_sec' not in ret
        assert 'version' in ret
        assert 'uptime' not in ret


def test_memcached_json(monkeypatch):
    mc = MemcachedWrapper(CounterMock().wrapper(), 'localhost')
    get = MagicMock()
    get.return_value = '{"foo": 1, "bar": 2}'
    monkeypatch.setattr('pymemcache.client.base.Client.get', get)
    ret = mc.json('somekey')
    assert ret == {'foo': 1, 'bar': 2}
