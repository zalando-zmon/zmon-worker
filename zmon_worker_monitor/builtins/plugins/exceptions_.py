#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Zalando-specific function to query the Exception Monitor
"""

from collections import Iterable

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial
from zmon_worker_monitor import plugin_manager


class ExceptionsFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(ExceptionsFactory, self).__init__()
        # fields from dependencies: plugin depends 1 other plugin
        self.http_factory = None

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
        entity = factory_ctx['entity']
        project = entity['name'] if entity['type'] == 'project' else None

        # load plugins dependencies and store them locally for efficiency
        if not self.http_factory:
            self.http_factory = plugin_manager.get_plugin_obj_by_name('http', 'Function')

        return propartial(ExceptionsWrapper,
                          http_wrapper=self.http_factory.create(factory_ctx),
                          host=factory_ctx['host'],
                          instance=factory_ctx['instance'],
                          project=project)


class ExceptionsWrapper(object):

    def __init__(self, http_wrapper, host=None, instance=None, project=None):
        self.__http = http_wrapper
        self.url = 'https://exceptions.example.com/'
        self.host = host
        self.instance = instance
        self.project = project

    def __request(self, path, **params):
        return self.__http(path, base_url=self.url, params=params).json()

    def count(
        self,
        host=None,
        instance=None,
        project=None,
        source_class=None,
        method_name=None,
        exception_class=None,
        time_from='-24h',
        time_to=None,
        level='ERROR',
        q=None,
    ):

        return self.__request(
            'count',
            host=maybe_comma_join(host or self.host),
            instance=maybe_comma_join(instance or self.instance),
            project=maybe_comma_join(project or self.project),
            source_class=maybe_comma_join(source_class),
            method_name=maybe_comma_join(method_name),
            exception_class=maybe_comma_join(exception_class),
            time_from=time_from,
            time_to=time_to,
            level=maybe_comma_join(level),
            q=q,
        )['count']


def maybe_comma_join(s):
    """
    If `s` is iterable (but not a string), returns a comma-separated Unicode string of the elements of `s`.
    Otherwise, returns `s`

    >>> maybe_comma_join(['a', 'b', 'c'])
    u'a,b,c'

    >>> maybe_comma_join([1, 2, 3])
    u'1,2,3'

    >>> maybe_comma_join([u'\u03B1', u'\u03B2', u'\u03B3'])
    u'\u03b1,\u03b2,\u03b3'

    >>> maybe_comma_join([])
    ''

    >>> maybe_comma_join('abc')
    'abc'

    >>> maybe_comma_join(u'\u03B1\u03B2\u03B3')
    u'\u03b1\u03b2\u03b3'

    >>> maybe_comma_join('')
    ''

    >>> maybe_comma_join(123)
    123
    """

    if isinstance(s, Iterable) and not isinstance(s, basestring):
        return ','.join(unicode(e) for e in s)
    else:
        return s


