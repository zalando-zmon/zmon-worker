import pytest

from mock import MagicMock, call

from zmon_worker_monitor.builtins.plugins.sql_postgresql import (
    SqlWrapper, REQUIRED_GROUP, CONNECTION_RE, DEFAULT_PORT, make_safe, PERMISSIONS_STMT, CheckError, DbError,
    InsufficientPermissionsError)

RESULT_COUNT = 2

STMT = 'SELECT * FROM X'


@pytest.fixture(params=[
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1', 'timeout': 180000},
        {'col1': 1, 'col2': 2, 'col3': 3},
        {'col1': 1, 'col2': 2, 'col3': 3},
        [{'col1': 1, 'col2': 2, 'col3': 3}] * RESULT_COUNT,
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db-2'}, 'shard': 'shard-1', 'created_by': 'zmon', 'timeout': 90000},
        {'col1': 1, 'col2': 2, 'col3': 3},
        {'col1': 1, 'col2': 2, 'col3': 3},
        [{'col1': 1, 'col2': 2, 'col3': 3}] * RESULT_COUNT,
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1', 'created_by': 'zmon', 'check_id': 10},
        {'col1': 1, 'col2': 2, 'col3': 3, 'col4': 'string - NaN'},
        {'col1': 1, 'col2': 2, 'col3': 3, 'col4': ['string - NaN']},
        [{'col1': 1, 'col2': 2, 'col3': 3, 'col4': 'string - NaN'}] * RESULT_COUNT,
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db', 'shard-2': 'pg-host-2:5432/db'}, 'created_by': 'zmon'},
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
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1', 'created_by': 'zmon'},
        {'connect_error': True},
        DbError
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1', 'created_by': 'zmon'},
        {'fetch_error': True},
        DbError
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1', 'created_by': 'zmon'},
        {'no_login': True},
        InsufficientPermissionsError
    ),
    (
        {'shards': {'shard-1': 'pg-host:5432/db'}, 'shard': 'shard-1', 'created_by': 'zmon'},
        {'no_group': True},
        InsufficientPermissionsError
    ),
])
def fx_sql_error(request):
    return request.param


def get_connection_str(shard_def, user='zmon', password='', connect_timeout=5,
                       created_by=None, check_id=None, **kwargs):
    m = CONNECTION_RE.match(shard_def)

    connection_str = ("host='{host}' port='{port}' dbname='{dbname}' user='{user}' password='{password}' "
                      "connect_timeout='{connect_timeout}' "
                      "application_name='ZMON Check {check_id} (created by {created_by})' ").format(
        host=m.group('host'),
        port=int(m.group('port') or DEFAULT_PORT),
        dbname=m.group('dbname'),
        user=user,
        password=password,
        connect_timeout=connect_timeout,
        check_id=check_id,
        created_by=make_safe(created_by),
    )

    return connection_str


def assert_connect(connect, kwargs):
    shards = kwargs['shards']
    shard = kwargs.get('shard')

    shard_def = shards.get(shard)

    shard_list = [shard_def] if shard_def else shards.values()

    calls = []
    for shard_def in shard_list:

        connection_str = get_connection_str(shard_def, **kwargs)

        calls.append(call(connection_str))

    connect.assert_has_calls(calls, any_order=True)
    connect.return_value.set_session.assert_called_with(readonly=True, autocommit=True)

    cursor_calls = [
        call("SET statement_timeout TO %s;", [kwargs.get('timeout', 60000)])
    ]
    if 'created_by' in kwargs:
        cursor_calls.append(call(PERMISSIONS_STMT, [kwargs['created_by']]))

    connect.return_value.cursor.return_value.execute.assert_has_calls(cursor_calls)


def mock_connect(connect_error=False, fetch_error=False, no_login=False, no_group=False, results={}):
    conn = MagicMock()
    cursor = MagicMock()
    row = MagicMock()

    row.can_login = False if no_login else True
    row.member_of = [] if no_group else [REQUIRED_GROUP]

    row._asdict.return_value = results

    if not fetch_error:
        cursor.fetchone.return_value = row
        cursor.fetchmany.return_value = [row] * RESULT_COUNT
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

    monkeypatch.setattr('psycopg2.connect', connect)

    sql = SqlWrapper(**kwargs)

    assert sql

    assert_connect(connect, kwargs)


def test_sql_wrapper_result(monkeypatch, fx_sql):
    kwargs, results, exp_res, _ = fx_sql

    connect = mock_connect(results=results)

    monkeypatch.setattr('psycopg2.connect', connect)

    sql = SqlWrapper(**kwargs)

    assert sql

    assert_connect(connect, kwargs)

    res = sql.execute(STMT).result()

    assert res == exp_res
    assert STMT == sql._stmt


def test_sql_wrapper_results(monkeypatch, fx_sql):
    kwargs, results, _, exp_results = fx_sql

    connect = mock_connect(results=results)

    monkeypatch.setattr('psycopg2.connect', connect)

    sql = SqlWrapper(**kwargs)

    assert sql

    assert_connect(connect, kwargs)

    res = sql.execute(STMT).results()

    assert res == exp_results

    res = sql.execute(STMT).results(raise_if_limit_exceeded=False)

    assert res == exp_results
    assert STMT == sql._stmt


def test_sql_wrapper_errors(monkeypatch, fx_sql_error):
    kwargs, errors, ex = fx_sql_error

    connect = mock_connect(**errors)
    monkeypatch.setattr('psycopg2.connect', connect)

    with pytest.raises(ex):
        SqlWrapper(**kwargs)


def test_sql_wrapper_result_errors(monkeypatch, fx_sql):
    kwargs, _, _, _ = fx_sql

    connect = mock_connect()
    monkeypatch.setattr('psycopg2.connect', connect)

    sql = SqlWrapper(**kwargs)

    connect.return_value.cursor.return_value.fetchone.side_effect = Exception()

    with pytest.raises(DbError):
        sql.execute(STMT).result()


def test_sql_wrapper_results_errors(monkeypatch, fx_sql):
    kwargs, _, _, _ = fx_sql

    connect = mock_connect()
    monkeypatch.setattr('psycopg2.connect', connect)

    sql = SqlWrapper(**kwargs)

    connect.return_value.cursor.return_value.fetchmany.side_effect = Exception()

    with pytest.raises(DbError):
        sql.execute(STMT).results()


def test_sql_wrapper_results_limit_exceeded(monkeypatch, fx_sql):
    kwargs, _, _, _ = fx_sql

    connect = mock_connect()
    monkeypatch.setattr('psycopg2.connect', connect)

    sql = SqlWrapper(**kwargs)

    with pytest.raises(DbError):
        sql.execute(STMT).results(max_results=RESULT_COUNT - 1)
