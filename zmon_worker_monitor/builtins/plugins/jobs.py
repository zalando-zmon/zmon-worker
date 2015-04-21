#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Zalando-specific function to query DeployCtl job information
"""

from itertools import groupby
from operator import itemgetter

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial
from zmon_worker_monitor import plugin_manager


class JobsFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(JobsFactory, self).__init__()
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

        # load plugins dependencies and store them locally for efficiency
        if not self.http_factory:
            self.http_factory = plugin_manager.get_plugin_obj_by_name('http', 'Function')

        return propartial(JobsWrapper,
                          http_wrapper=self.http_factory.create(factory_ctx),
                          project=factory_ctx['entity'].get('name'))


class JobsWrapper(object):

    def __init__(self, http_wrapper, environment, project, **kwargs):
        self.url = 'https://deployctl.example.com/jobs/history.json/{}/{}'.format(environment, project)
        self.__http = http_wrapper
        self.http_wrapper_params = kwargs
        self.name = itemgetter('name')

    def __request(self):
        return self.__http(self.url, **self.http_wrapper_params).json()

    def lastruns(self):
        start_time = itemgetter('start_seconds_ago')

        return dict((job, min(runs, key=start_time)) for (job, runs) in groupby(sorted(self.__request(),
                    key=self.name), key=self.name))

    def history(self):
        return dict((job, list(runs)) for (job, runs) in groupby(sorted(self.__request(), key=self.name),
                    key=self.name))


