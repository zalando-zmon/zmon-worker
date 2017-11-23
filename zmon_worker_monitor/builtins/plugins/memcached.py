#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymemcache.client.base import Client

from zmon_worker_monitor.zmon_worker.errors import ConfigurationError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial
from zmon_worker_monitor import plugin_manager

STRING_KEYS = frozenset([
    'version',
    'libevent',
])


class MemcachedFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(MemcachedFactory, self).__init__()
        # fields to store dependencies: plugin depends on 1 other plugin
        self.counter_factory = None

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
        return propartial(MemcachedWrapper, host=factory_ctx['host'])


class MemcachedWrapper(object):
    '''Class to allow only readonly access to underlying redis connection'''

    def __init__(self, host, port=11211, socket_connect_timeout=1):
        if not host:
            raise ConfigurationError('Memcached wrapper improperly configured. Valid redis host is required!')

        self.__con = Client((host, port))

    def __del__(self):
        self.__con.quit()

    def get(self, key):
        return self.__con.get(key)

    def stats(self):
        return self.__con.stats()


if __name__ == '__main__':
    import sys
    import json

    # init plugin manager and collect plugins, as done by Zmon when worker is starting
    plugin_manager.init_plugin_manager()

    factory_ctx = {
            'host': 'localhost',
    }
    wrapper = MemcachedWrapper(sys.argv[1])
    print json.dumps(wrapper.stats(), indent=4, sort_keys=True)
