import pytest

from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.dns_ import DnsWrapper, ConfigurationError


def test_dns_resolve(monkeypatch):
    gethost = MagicMock()
    gethost.return_value = '192.168.20.16'

    monkeypatch.setattr('socket.gethostbyname', gethost)

    dns = DnsWrapper(host=None)
    res = dns.resolve('google.de')

    assert res == gethost.return_value

    dns = DnsWrapper(host='google.de')
    res = dns.resolve()

    assert res == gethost.return_value


def test_dns_resolve_exception(monkeypatch):
    gethost = MagicMock()
    gethost.side_effect = RuntimeError

    monkeypatch.setattr('socket.gethostbyname', gethost)

    dns = DnsWrapper(host=None)
    res = dns.resolve('google.de')

    assert 'ERROR' in res


def test_dns_config_error():
    with pytest.raises(ConfigurationError):
        dns = DnsWrapper(host=None)
        dns.resolve()


def test_dns_query(monkeypatch):
    resolver = MagicMock()
    query = MagicMock()
    query.return_value = ['192.168.20.16']
    resolver.query = query
    resolver_class = MagicMock()
    resolver_class.return_value = resolver
    monkeypatch.setattr('dns.resolver.Resolver', resolver_class)

    dns = DnsWrapper(host=None)
    res = dns.query('google.de', 'A')

    assert res == ['192.168.20.16']
    query.assert_called_with('google.de', 'A')


@pytest.mark.parametrize('kwargs', [{'host': None}, {'host': 'www.google.com', 'recordtype': None}])
def test_dns_query_config_error(kwargs):
    with pytest.raises(ConfigurationError):
        dns = DnsWrapper('www.example.org')
        dns.query(**kwargs)
