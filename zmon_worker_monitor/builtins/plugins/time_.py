#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime

from zmon_worker_monitor.zmon_worker.common.time_ import parse_timedelta, parse_datetime

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

class TimeFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(TimeFactory, self).__init__()

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
        return propartial(TimeWrapper)

class TimeWrapper(object):

    def __init__(self, spec='now', utc=False):
        now = (datetime.utcnow() if utc else datetime.now())
        delta = parse_timedelta(spec)
        if delta:
            self.time = now + delta
        elif spec == 'now':
            self.time = now
        else:
            self.time = parse_datetime(spec)

    def __sub__(self, other):
        '''
        >>> TimeWrapper('2014-01-01 01:01:25') - TimeWrapper('2014-01-01 01:01:01')
        24.0
        '''

        return (self.time - other.time).total_seconds()

    def isoformat(self, sep=' '):
        return self.time.isoformat(sep)

    def format(self, fmt):
        '''
        >>> TimeWrapper('2014-01-01 01:01').format('%Y-%m-%d')
        '2014-01-01'

        >>> TimeWrapper('-1m').format('%Y-%m-%d')[:3]
        '201'
        '''

        return self.time.strftime(fmt)
