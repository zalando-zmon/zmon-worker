#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

# from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

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

        self._username = conf.get('user', None)
        self._password = conf.get('password', None)

        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(CassandraWrapper, node=factory_ctx.get('host'), username=self._username, password=self._password)


class CassandraWrapper(object):
    def __init__(self, node, keyspace, username=None, password=None, port=9042, connect_timeout=1):
        # for now using a single host / node should be seed nodes or at least available nodes
        self.node = node
        self.port = port
        self.__username = username
        self.__password = password
        self.keyspace = keyspace
        self.connect_timeout = connect_timeout

    def execute(self, stmt):
        auth_provider = None
        if self.__username and self.__password:
            auth_provider = PlainTextAuthProvider(username=self.__username, password=self.__password)

        cl = Cluster([self.node], connect_timeout=self.connect_timeout, auth_provider=auth_provider)
        # cl.connection_class = LibevConnection

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
            cl.shutdown()

        return {}
