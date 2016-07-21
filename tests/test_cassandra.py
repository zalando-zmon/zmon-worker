import pytest
from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.cassandra_wrapper import CassandraWrapper


@pytest.mark.parametrize('kwargs', [{'username': 'user', 'password': 'pass'}, {}])
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
    if kwargs:
        auth_provider = auth.return_value
        auth.assert_called_with(username=kwargs['username'], password=kwargs['password'])

    cluster.assert_called_with([node], connect_timeout=cassandra.connect_timeout, auth_provider=auth_provider)

    client.connect.assert_called_once()
    client.shutdown.assert_called_once()

    session.set_keyspace.assert_called_with(keyspace)
    session.execute.assert_called_with(stmt)


def test_cassandra_execute_exception(monkeypatch):
    node = 'cassandra-node'
    keyspace = 'users'

    result = {}

    client = MagicMock()
    cluster = MagicMock()

    cluster.return_value = client

    client.connect.side_effect = RuntimeError

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.cassandra_wrapper.Cluster', cluster)

    cassandra = CassandraWrapper(node, keyspace)

    with pytest.raises(RuntimeError):
        res = cassandra.execute('SELECT')

        assert res == result

        client.connect.assert_called_once()
        client.shutdown.assert_called_once()
