import pytest

from zmon_worker_monitor.builtins.plugins.checkldap import LdapWrapper, ConfigurationError


# TODO: more tests!


def test_ldap_error():
    with pytest.raises(ConfigurationError):
        LdapWrapper(counter=lambda x: x)
