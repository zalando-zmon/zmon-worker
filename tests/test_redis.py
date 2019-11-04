import pytest
import zmon_worker_monitor.builtins.plugins.redis_wrapper as rediswrapper

from mock import MagicMock
from redis.exceptions import ConnectionError

STATS = {
    "blocked_clients": 2,
    "commands_processed_per_sec": 15946.48,
    "connected_clients": 162,
    "connected_slaves": 0,
    "connections_received_per_sec": 0.5,
    "dbsize": 27351,
    "evicted_keys_per_sec": 0.0,
    "expired_keys_per_sec": 0.0,
    "instantaneous_ops_per_sec": 29626,
    "keyspace_hits_per_sec": 1195.43,
    "keyspace_misses_per_sec": 1237.99,
    "maxmemory": 152343648,
    "used_memory": 50781216,
    "used_memory_rss": 63475712,
    "used_memory_lua": 475712,
    "foo": "bar"
}


def test_redis_config_error():
    with pytest.raises(rediswrapper.ConfigurationError):
        rediswrapper.RedisWrapper(lambda x: x, None)

    with pytest.raises(rediswrapper.ConfigurationError):
        rediswrapper.RedisWrapper(lambda x: x, '')


def test_redis_connection_exception(monkeypatch):
    host = 'redis-node'
    connect = MagicMock()

    monkeypatch.setattr('redis.connection.Connection.connect', connect)
    connect.side_effect = ConnectionError
    redis = rediswrapper.RedisWrapper(lambda x: x, host)
    with pytest.raises(ConnectionError):
        redis.statistics()


@pytest.mark.parametrize('kwargs', [
    {'host': 'redis-host', 'password': 'pass', 'port': 19042},
    {'host': 'redis-host', 'port': 100500}
])
def test_redis_parametrized(monkeypatch, kwargs):

    rediswrapper.STATISTIC_COUNTER_KEYS = frozenset()
    rediswrapper.STATISTIC_GAUGE_KEYS = STATS.keys()

    def get(key):
        if key in STATS:
            return STATS[key]
        return None

    def info():
        return STATS

    def dbsize():
        return STATS['dbsize']

    redisMock = MagicMock()
    redisMock().dbsize.side_effect = dbsize
    redisMock().get.side_effect = get
    redisMock().info.side_effect = info

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.redis_wrapper.redis.StrictRedis', redisMock)
    wrapper = rediswrapper.RedisWrapper(
        counter=lambda x: x,
        **kwargs)
    assert wrapper.get('foo') == 'bar'
    assert wrapper.get('test') is None
    assert wrapper.statistics() == STATS
    redisMock.assert_called_with(
        kwargs.get('host'),
        kwargs.get('port'),
        kwargs.get('db', 0),
        kwargs.get('password'),
        socket_connect_timeout=kwargs.get('socket_connect_timeout', 1),
        socket_timeout=kwargs.get('socket_timeout', 5),
        ssl=False,
        ssl_cert_reqs='required'
    )
