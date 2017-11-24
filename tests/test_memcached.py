import pytest

from zmon_worker_monitor.builtins.plugins.memcached import MemcachedWrapper, ConfigurationError


def test_memcached_config_error():
    with pytest.raises(ConfigurationError):
        MemcachedWrapper(lambda x: x, None)

    with pytest.raises(ConfigurationError):
        MemcachedWrapper(lambda x: x, '')
