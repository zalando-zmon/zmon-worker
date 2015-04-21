#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Zalando-specific function to query EventLog
"""

from zmon_worker_monitor.zmon_worker.errors import CheckError
#from http import HttpWrapper  # FIXME: watch out for this!!!

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial
from zmon_worker_monitor import plugin_manager


class EventlogFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(EventlogFactory, self).__init__()
        # fields from configuration
        self.eventlog_url = None
        # fields from dependencies: plugin depends 1 other plugin
        self.http_factory = None

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        self.eventlog_url = conf['eventlog_url']

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """

        # load plugins dependencies and store them locally for efficiency
        if not self.http_factory:
            self.http_factory = plugin_manager.get_plugin_obj_by_name('http', 'Function')

        return propartial(EventLogWrapper,
                          http_wrapper=self.http_factory.create(factory_ctx),
                          url=self.eventlog_url)


class EventLogWrapper(object):

    '''Convenience wrapper to access EventLog counts'''

    def __init__(self, http_wrapper, url):
        self.__http = http_wrapper
        self.url = url.rstrip('/') + '/'

    def __request(self, path, params):
        return self.__http(self.url + path, params=params).json()

    def count(self, event_type_ids, time_from, time_to=None, group_by=None, **kwargs):
        '''Return number of events for given type IDs in given time frame

        returns a single number (integer) if only one type ID is given
        returns a dict (typeId as hex=>count) if more than one type ID is given
        returns a dict (fieldValue => count) if one type ID is given and a field name with "group_by"

        >>> EventLogWrapper(object, 'https://eventlog.example.com/').count('a', time_from='-1h')
        Traceback (most recent call last):
            ...
        CheckError: EventLog type ID must be a integer

        >>> EventLogWrapper(object, 'https://eventlog.example.com/').count(123, time_from='-1h')
        Traceback (most recent call last):
            ...
        CheckError: EventLog type ID is out of range
        '''

        if isinstance(event_type_ids, (int, long)):
            event_type_ids = [event_type_ids]
        for type_id in event_type_ids:
            if not isinstance(type_id, (int, long)):
                raise CheckError('EventLog type ID must be a integer')
            if type_id < 0x1001 or type_id > 0xfffff:
                raise CheckError('EventLog type ID is out of range')
        params = kwargs
        params['event_type_ids'] = ','.join(['{:x}'.format(v) for v in event_type_ids])
        params['time_from'] = time_from
        params['time_to'] = time_to
        params['group_by'] = group_by
        result = self.__request('count', params)
        if len(event_type_ids) == 1 and not group_by:
            return result.get(params['event_type_ids'], 0)
        else:
            return result


if __name__ == '__main__':

    import sys
    import logging
    logging.basicConfig(level=logging.DEBUG)

    # init plugin manager and collect plugins, as done by Zmon when worker is starting
    plugin_manager.init_plugin_manager()
    plugin_manager.collect_plugins(load_builtins=True, load_env=True)

    eventlog_url = sys.argv[1]
    factory_ctx = {}

    http = plugin_manager.get_plugin_obj_by_name('http', 'Function').create(factory_ctx)

    #eventlog = EventLogWrapper()
    eventlog = EventLogWrapper(http_wrapper=http, url=eventlog_url)

    print eventlog.count(0x96001, time_from='-1m')
    print eventlog.count([0x96001, 0x63005], time_from='-1m')
    print eventlog.count(0x96001, time_from='-1m', group_by='appDomainId')
