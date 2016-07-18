import pytest

from zmon_worker_monitor.builtins.plugins.mongodb import MongoDBWrapper, ConfigurationError


def test_mongodb_config_error():
    with pytest.raises(ConfigurationError):
        MongoDBWrapper(None)
