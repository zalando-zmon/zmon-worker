import pytest

from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.bigquery import BigqueryWrapper, ConfigurationError


@pytest.fixture(params=[
    (
        'SELECT visit_date, searches_per_day from searches',
        [{'visit_date': 0, 'searches_per_day': 0}],
        [{'visit_date': 0, 'searches_per_day': 0}]
    )
])
def fx_query(request):
    return request.param


def test_bigquery_config_error(monkeypatch):
    with pytest.raises(ConfigurationError):
        BigqueryWrapper('')


def test_bigquery_query(monkeypatch, fx_query):
    kwargs, res, exp = fx_query
    monkeypatch.setattr('google.oauth2.service_account.Credentials', MagicMock())
    clientMock = MagicMock()
    clientMock.return_value.query.return_value.result.return_value = res
    monkeypatch.setattr('google.cloud.bigquery.Client', clientMock)
    bigquery_key = '123'
    bigqueryWrapper = BigqueryWrapper(bigquery_key)

    query_result = bigqueryWrapper.query(kwargs)

    assert query_result == exp
