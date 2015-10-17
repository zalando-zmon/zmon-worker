#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

#from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.cassandra-function')

class CassandraFactory(IFunctionFactoryPlugin):

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
        return propartial(CassandraWrapper, node=factory_ctx.get('host'))

class CassandraWrapper(object):

    def __init__(self, node, keyspace, port=9042, connect_timeout=1):
        # for now using a single host / node should be seed nodes or at least available nodes
        self.node = node
        self.port = port
        self.keyspace = keyspace
        self.connect_timeout = connect_timeout

    def execute(self, stmt):
        cl = Cluster([self.node], connect_timeout=self.connect_timeout)
        #cl.connection_class = LibevConnection

        session = None
        try:
            session = cl.connect()
            session.set_keyspace(self.keyspace)

            rs = session.execute(stmt)

            result = []

            for r in rs:
                result.append(r)

            return result

        finally:
            cl.shutdown();

        return {}