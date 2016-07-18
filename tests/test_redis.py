import pytest

from zmon_worker_monitor.builtins.plugins.redis_wrapper import RedisWrapper, ConfigurationError


def test_redis_config_error():
    with pytest.raises(ConfigurationError):
        RedisWrapper(lambda x: x, None)

    with pytest.raises(ConfigurationError):
        RedisWrapper(lambda x: x, '')
