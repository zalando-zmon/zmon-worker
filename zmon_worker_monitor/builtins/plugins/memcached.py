#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymemcache.client.base import Client

from zmon_worker_monitor.zmon_worker.errors import ConfigurationError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial
from zmon_worker_monitor import plugin_manager

COUNTER_KEYS = frozenset([
    'total_connections',
    'rejected_connections',
    'cmd_get',
    'cmd_set',
    'cmd_flush',
    'cmd_touch',
    'cmd_config_get',
    'cmd_config_set',
    'get_hits',
    'get_misses',
    'get_expired',
    'get_flushed',
    'delete_misses',
    'delete_hits',
    'incr_misses',
    'incr_hits',
    'decr_misses',
    'decr_hits',
    'cas_misses',
    'cas_hits',
    'cas_badval',
    'touch_hits',
    'touch_misses',
    'auth_cmds',
    'auth_errors',
    'bytes_read',
    'bytes_written',
    'conn_yields',
    'slab_reassign_rescues',
    'slab_reassign_chunk_rescues',
    'slab_reassign_evictions_nomem',
    'slab_reassign_inline_reclaim',
    'slab_reassign_busy_items',
    'slab_reassign_running',
    'slabs_moved',
    'lru_crawler_starts',
    'lru_maintainer_juggles',
    'malloc_fails',
    'log_worker_dropped',
    'log_worker_written',
    'log_watcher_skipped',
    'log_watcher_sent',
    'crawler_reclaimed',
    'evictions',
    'reclaimed',
    'crawler_items_checked',
    'lrutail_reflocked',
    'moves_to_cold',
    'moves_to_warm',
    'moves_within_lru',
    'direct_reclaims',
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
        # load plugins dependencies and store them locally for efficiency
        if not self.counter_factory:
            self.counter_factory = plugin_manager.get_plugin_obj_by_name('counter', 'Function')
        return propartial(MemcachedWrapper, counter=self.counter_factory.create(factory_ctx), host=factory_ctx['host'])


class MemcachedWrapper(object):
    '''Class to allow only readonly access to underlying redis connection'''

    def __init__(self, counter, host, port=11211, socket_connect_timeout=1):
        if not host:
            raise ConfigurationError('Memcached wrapper improperly configured. Valid redis host is required!')

        self.__con = Client((host, port))
        self._counter = counter('')

    def __del__(self):
        self.__con.quit()

    def get(self, key):
        return self.__con.get(key)

    def stats(self):
        data = self.__con.stats()
        ret = {}
        for key in data:
            if key in COUNTER_KEYS:
                ret['{}_per_sec'.format(key.replace('total_', ''))] = \
                    round(self._counter.key(key).per_second(data.get(key, 0)), 2)
            else:
                ret[key] = data[key]


if __name__ == '__main__':
    import sys
    import json

    # init plugin manager and collect plugins, as done by Zmon when worker is starting
    plugin_manager.init_plugin_manager()
    plugin_manager.collect_plugins(load_builtins=True, load_env=True)

    factory_ctx = {
            'host': 'localhost',
    }
    counter = plugin_manager.get_plugin_obj_by_name('counter', 'Function').create(factory_ctx)
    wrapper = MemcachedWrapper(counter, sys.argv[1])
    print json.dumps(wrapper.stats(), indent=4, sort_keys=True)
