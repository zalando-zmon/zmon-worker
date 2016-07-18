import pytest

from zmon_worker_monitor.builtins.plugins.jmx import JmxWrapper, ConfigurationError

# TODO More tests!


def test_jmx_config_error():
    with pytest.raises(ConfigurationError):
        JmxWrapper(None, None, 'host', 2222)

    with pytest.raises(ConfigurationError):
        JmxWrapper(None, 1234, 'host', 2222)
