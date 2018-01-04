import pytest
from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.cassandra_wrapper import CassandraWrapper


@pytest.mark.parametrize('kwargs', [{'username': 'user', 'password': 'pass', 'port': 19042, 'protocol_version': 4}, {}])
def test_cassandra_execute(monkeypatch, kwargs):
    node = 'cassandra-node'
    keyspace = 'users'

    stmt = 'SELECT'
    result = [1, 2, 3]

    client = MagicMock()
    cluster = MagicMock()
    session = MagicMock()
    auth = MagicMock()

    cluster.return_value = client

    client.connect.return_value = session

    session.execute.return_value = result

    auth.return_value = 'auth'

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.cassandra_wrapper.Cluster', cluster)
    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.cassandra_wrapper.PlainTextAuthProvider', auth)

    cassandra = CassandraWrapper(node, keyspace, **kwargs)

    res = cassandra.execute(stmt)

    assert res == result

    auth_provider = None
    port = 9042
    protocol_version = 3
    if kwargs:
        auth_provider = auth.return_value
        auth.assert_called_with(username=kwargs['username'], password=kwargs['password'])
        port = kwargs['port']
        protocol_version = kwargs['protocol_version']

    cluster.assert_called_with([node], connect_timeout=cassandra.connect_timeout, auth_provider=auth_provider,
                               port=port, protocol_version=protocol_version)

    cassandra = None

    client.connect.assert_called_once()
    client.shutdown.assert_called_once()

    session.set_keyspace.assert_called_with(keyspace)
    session.execute.assert_called_with(stmt)


def test_cassandra_connect_exception(monkeypatch):
    node = 'cassandra-node'
    keyspace = 'users'

    client = MagicMock()
    cluster = MagicMock()
    session = MagicMock()

    cluster.return_value = client

    client.connect.return_value = session
    client.connect.side_effect = RuntimeError

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.cassandra_wrapper.Cluster', cluster)

    with pytest.raises(RuntimeError):
        cassandra = CassandraWrapper(node, keyspace)
        cassandra.execute('SELECT')

    client.connect.assert_called_once()
    client.shutdown.assert_not_called()
    session.execute.assert_not_called()


def test_cassandra_execute_exception(monkeypatch):
    node = 'cassandra-node'
    keyspace = 'users'

    client = MagicMock()
    cluster = MagicMock()
    session = MagicMock()

    cluster.return_value = client

    client.connect.return_value = session

    session.execute.side_effect = RuntimeError

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.cassandra_wrapper.Cluster', cluster)

    with pytest.raises(RuntimeError):
        def f():
            cassandra = CassandraWrapper(node, keyspace)
            cassandra.execute('SELECT')
        f()

    session.execute.assert_called_with('SELECT')
    client.connect.assert_called_once()
    client.shutdown.assert_called_once()
