import pytest

from mock import MagicMock, call

from zmon_worker_monitor.builtins.plugins.sql_mysql import (
    MySqlWrapper, CONNECTION_RE, DEFAULT_PORT, DbError, CheckError)

RESULT_COUNT = 2

STMT = 'SELECT * FROM X'


@pytest.fixture(params=[
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1'},
        {'col1': 1, 'col2': 2, 'col3': 3},
        {'col1': 1, 'col2': 2, 'col3': 3},
        [{'col1': 1, 'col2': 2, 'col3': 3}] * RESULT_COUNT,
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1'},
        {'col1': 1, 'col2': 2, 'col3': 3},
        {'col1': 1, 'col2': 2, 'col3': 3},
        [{'col1': 1, 'col2': 2, 'col3': 3}] * RESULT_COUNT,
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1'},
        {'col1': 1, 'col2': 2, 'col3': 3, 'col4': 'string - NaN'},
        {'col1': 1, 'col2': 2, 'col3': 3, 'col4': ['string - NaN']},
        [{'col1': 1, 'col2': 2, 'col3': 3, 'col4': 'string - NaN'}] * RESULT_COUNT,
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db', 'shard-2': 'pg-host-2:5432/db'}},
        {'col1': 1, 'col2': 2, 'col3': 3, 'col4': 'string - NaN'},
        {'col1': 2, 'col2': 4, 'col3': 6, 'col4': ['string - NaN', 'string - NaN']},
        [{'col1': 1, 'col2': 2, 'col3': 3, 'col4': 'string - NaN'}] * RESULT_COUNT * 2,
    ),
])
def fx_sql(request):
    return request.param


@pytest.fixture(params=[
    (
        {'shards': None},
        {},
        CheckError
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'not-existing'},
        {},
        CheckError
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432db'}, 'shard': 'shard-1'},
        {},
        CheckError
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432//db'}, 'shard': 'shard-1'},
        {},
        CheckError
    ),
    (
        {'shards': {'shard-1': ':5432/db'}, 'shard': 'shard-1'},
        {},
        CheckError
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1'},
        {'connect_error': True},
        DbError
    ),
])
def fx_sql_error(request):
    return request.param


def assert_connect(connect, kwargs):
    shards = kwargs['shards']
    shard = kwargs.get('shard')

    shard_def = shards.get(shard)

    shard_list = [shard_def] if shard_def else shards.values()

    calls = []
    for shard_def in shard_list:

        m = CONNECTION_RE.match(shard_def)

        connection_kwargs = {
            'host': m.group('host'),
            'user': kwargs.get('user', 'nagios'),
            'passwd': kwargs.get('password', ''),
            'db': m.group('dbname'),
            'port': int(m.group('port')) if int(m.group('port')) > 0 else DEFAULT_PORT,
            'connect_timeout': kwargs.get('timeout', 60000)
        }

        calls.append(call(**connection_kwargs))

    connect.assert_has_calls(calls, any_order=True)
    connect.return_value.autocommit.assert_called_with(True)


def mock_connect(connect_error=False, fetch_error=False, no_login=False, no_group=False, results={}):
    conn = MagicMock()
    cursor = MagicMock()

    if not fetch_error:
        cursor.fetchone.return_value = results
        cursor.fetchmany.return_value = [results] * RESULT_COUNT
    else:
        cursor.fetchone.side_effect = Exception()
        cursor.fetchmany.side_effect = Exception()

    conn.cursor = MagicMock()
    conn.cursor.return_value = cursor

    connect = MagicMock()
    if not connect_error:
        connect.return_value = conn
    else:
        connect.side_effect = Exception()

    return connect


def test_sql_wrapper(monkeypatch, fx_sql):
    kwargs, _, _, _ = fx_sql

    connect = mock_connect()

    monkeypatch.setattr('pymysql.connect', connect)

    sql = MySqlWrapper(**kwargs)

    assert sql

    assert_connect(connect, kwargs)


def test_sql_wrapper_result(monkeypatch, fx_sql):
    kwargs, results, exp_res, _ = fx_sql

    connect = mock_connect(results=results)

    monkeypatch.setattr('pymysql.connect', connect)

    sql = MySqlWrapper(**kwargs)

    assert sql

    assert_connect(connect, kwargs)

    res = sql.execute(STMT).result()

    assert res == exp_res
    assert STMT == sql._stmt


def test_sql_wrapper_results(monkeypatch, fx_sql):
    kwargs, results, _, exp_results = fx_sql

    connect = mock_connect(results=results)

    monkeypatch.setattr('pymysql.connect', connect)

    sql = MySqlWrapper(**kwargs)

    assert sql

    assert_connect(connect, kwargs)

    res = sql.execute(STMT).results()

    assert res == exp_results

    res = sql.execute(STMT).results()

    assert res == exp_results
    assert STMT == sql._stmt


def test_sql_wrapper_errors(monkeypatch, fx_sql_error):
    kwargs, errors, ex = fx_sql_error

    connect = mock_connect(**errors)
    monkeypatch.setattr('pymysql.connect', connect)

    with pytest.raises(ex):
        MySqlWrapper(**kwargs)


def test_sql_wrapper_result_errors(monkeypatch, fx_sql):
    kwargs, _, _, _ = fx_sql

    connect = mock_connect()
    monkeypatch.setattr('pymysql.connect', connect)

    sql = MySqlWrapper(**kwargs)

    connect.return_value.cursor.return_value.fetchone.side_effect = Exception()

    with pytest.raises(DbError):
        sql.execute(STMT).result()


def test_sql_wrapper_results_errors(monkeypatch, fx_sql):
    kwargs, _, _, _ = fx_sql

    connect = mock_connect()
    monkeypatch.setattr('pymysql.connect', connect)

    sql = MySqlWrapper(**kwargs)

    connect.return_value.cursor.return_value.fetchmany.side_effect = Exception()

    with pytest.raises(DbError):
        sql.execute(STMT).results()
