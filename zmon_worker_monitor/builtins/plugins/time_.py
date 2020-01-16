#!/usr/bin/env python
# -*- coding: utf-8 -*-

from numbers import Number
from datetime import datetime

import pytz
import tzlocal

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

    EPOCH = datetime.fromtimestamp(0, pytz.UTC)

    def __init__(self, spec='now', utc=False, tz_name=None):
        if utc and tz_name:
            raise ValueError('Ambiguous time zone. Do not use "utc" and "tz_name" parameter at the same time.')

        tz = pytz.timezone(tz_name) if tz_name else None
        if utc:
            self.timezone = pytz.UTC
        elif tz_name:
            self.timezone = tz
        else:
            self.timezone = tzlocal.get_localzone()

        if isinstance(spec, Number):
            self.time = datetime.utcfromtimestamp(spec) if utc else datetime.fromtimestamp(spec, tz)
        else:
            now = datetime.utcnow() if utc else datetime.now(tz)
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
        '202'
        '''

        return self.time.strftime(fmt)

    def astimezone(self, tz_name):
        '''
        >>> TimeWrapper('2014-01-01 01:01', tz_name='UTC').astimezone('Europe/Berlin').isoformat()
        '2014-01-01 02:01:00+01:00'
        '''
        tz = pytz.timezone(tz_name)

        dt_with_tz = self.time if self.time.tzinfo else self.time.replace(tzinfo=self.timezone)

        epoch_seconds = (dt_with_tz.astimezone(tz) - self.EPOCH).total_seconds()
        return TimeWrapper(epoch_seconds, tz_name=tz_name)
