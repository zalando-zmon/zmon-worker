#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.mongodb-function')

class MongoDBFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(CassandraFactory, self).__init__()

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
        return propartial(KairosdbFactory, host=factory_ctx.get('host'))

class MongoDBWrapper(object):

    def __init__(self, host):
        self.host = host

    def execute(self, stmt):
        return {}