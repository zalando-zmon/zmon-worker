import pytest

from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.dns import DnsWrapper, ConfigurationError


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
