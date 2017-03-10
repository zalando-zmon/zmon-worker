import socket
import dns.resolver

from zmon_worker_monitor.zmon_worker.errors import ConfigurationError
from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


class DnsFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(DnsFactory, self).__init__()

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(DnsWrapper, host=factory_ctx['host'])


class DnsWrapper(object):

    def __init__(self, host, timeout=10):
        self.host = host
        self.timeout = timeout

    def resolve(self, host=None):
        if not self.host and not host:
            raise ConfigurationError('DNS wrapper improperly configured. Host is required!')

        if not host:
            host = self.host

        try:
            result = socket.gethostbyname(host)
        except Exception, e:
            result = 'ERROR: ' + str(e)
        return result

    def query(self, host=None, recordtype=None, nameserver=None):
        if not self.host and not host:
            raise ConfigurationError('DNS wrapper improperly configured. Host is required!')

        if not recordtype:
            raise ConfigurationError('DNS wrapper improperly configure. Recordtype is required!')

        resolver = dns.resolver.Resolver()
        # Allow to use custom nameserver
        if nameserver:
            resolver.nameservers = nameserver

        query = resolver.query(host, recordtype)
        result = []

        for response in query:
            result.append(str(response).replace('"', ''))

        return result
