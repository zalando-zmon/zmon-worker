#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis

from zmon_worker_monitor.zmon_worker.errors import ConfigurationError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial
from zmon_worker_monitor import plugin_manager

STATISTIC_GAUGE_KEYS = frozenset([
    'blocked_clients',
    'connected_clients',
    'connected_slaves',
    'instantaneous_ops_per_sec',
    'used_memory',
    'used_memory_rss',
])
STATISTIC_COUNTER_KEYS = frozenset([
    'evicted_keys',
    'expired_keys',
    'keyspace_hits',
    'keyspace_misses',
    'total_commands_processed',
    'total_connections_received',
])


class RedisFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(RedisFactory, self).__init__()
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
        # load plugins dependencies and store them locally for efficiency
        if not self.counter_factory:
            self.counter_factory = plugin_manager.get_plugin_obj_by_name('counter', 'Function')

        return propartial(RedisWrapper, counter=self.counter_factory.create(factory_ctx), host=factory_ctx['host'])


class RedisWrapper(object):
    '''Class to allow only readonly access to underlying redis connection'''

    def __init__(self, counter, host, port=6379, db=0, socket_connect_timeout=1):
        if not host:
            raise ConfigurationError('Redis wrapper improperly configured. Valid redis host is required!')

        self._counter = counter('')
        self.__con = redis.StrictRedis(host, port, db, socket_connect_timeout=socket_connect_timeout)

    def llen(self, key):
        return self.__con.llen(key)

    def lrange(self, key, start, stop):
        return self.__con.lrange(key, start, stop)

    def get(self, key):
        return self.__con.get(key)

    def hget(self, key, field):
        return self.__con.hget(key, field)

    def hgetall(self, key):
        return self.__con.hgetall(key)

    def scan(self, cursor, match=None, count=None):
        return self.__con.scan(cursor, match=match, count=count)

    def ttl(self, key):
        return self.__con.ttl(key)

    def keys(self, pattern):
        return self.__con.keys(pattern)

    def smembers(self, key):
        return self.__con.smembers(key)

    def scard(self, key):
        return self.__con.scard(key)

    def zcard(self, key):
        return self.__con.zcard(key)

    def statistics(self):
        '''
        Return general Redis statistics such as operations/s

        Example result::

            {
                "blocked_clients": 2,
                "commands_processed_per_sec": 15946.48,
                "connected_clients": 162,
                "connected_slaves": 0,
                "connections_received_per_sec": 0.5,
                "dbsize": 27351,
                "evicted_keys_per_sec": 0.0,
                "expired_keys_per_sec": 0.0,
                "instantaneous_ops_per_sec": 29626,
                "keyspace_hits_per_sec": 1195.43,
                "keyspace_misses_per_sec": 1237.99,
                "used_memory": 50781216,
                "used_memory_rss": 63475712
            }
        '''

        data = self.__con.info()
        stats = {}
        for key in STATISTIC_GAUGE_KEYS:
            stats[key] = data.get(key)
        for key in STATISTIC_COUNTER_KEYS:
            stats['{}_per_sec'.format(key).replace('total_', '')] = \
                round(self._counter.key(key).per_second(data.get(key, 0)), 2)
        stats['dbsize'] = self.__con.dbsize()
        return stats


if __name__ == '__main__':
    import sys
    import json

    # init plugin manager and collect plugins, as done by Zmon when worker is starting
    plugin_manager.init_plugin_manager()
    plugin_manager.collect_plugins(load_builtins=True, load_env=True)

    factory_ctx = {
        'redis_host': 'localhost',
    }
    counter = plugin_manager.get_plugin_obj_by_name('counter', 'Function').create(factory_ctx)
    wrapper = RedisWrapper(counter, sys.argv[1])
    print json.dumps(wrapper.statistics(), indent=4, sort_keys=True)
