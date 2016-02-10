#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import redis

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger(__name__)


class ZmonFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(ZmonFactory, self).__init__()

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
        return propartial(ZmonWrapper, factory_ctx['redis_host'], factory_ctx['redis_port'])


class ZmonWrapper(object):

    ZMON_ALERTS_ENTITIES_PATTERN = 'zmon:alerts:*:entities'

    def __init__(self, host, port):
        self.__redis = redis.StrictRedis(host, port)
        self.logger = logger

    def check_entities_total(self):
        '''
        Returns total number of checked entities.
        '''

        alert_entities = self.__redis.keys(self.ZMON_ALERTS_ENTITIES_PATTERN)
        p = self.__redis.pipeline()
        for key in alert_entities:
            p.hkeys(key)
        entities = p.execute()

        return sum(len(e) for e in entities)
