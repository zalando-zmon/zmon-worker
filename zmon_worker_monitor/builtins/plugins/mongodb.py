#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pymongo import MongoClient

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.mongodb-function')

class MongoDBFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(MongoDBFactory, self).__init__()

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
        return propartial(MongoDBWrapper, host=factory_ctx.get('host'))

class MongoDBWrapper(object):

    def __init__(self, host, port=27017):
        self.host = host
        self.port = port

    def find(self, database, collection, query, limit=50):
        client = MongoClient(self.host, self.port)
        try:
            db = client[database]
            rs = db[collection].find(query, limit=limit)
            result = []

            for r in rs:
                result.append(r)

            return result

        finally:
            client.close()
