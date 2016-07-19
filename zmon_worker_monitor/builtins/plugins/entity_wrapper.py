#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
import sys
import urllib
import urlparse
import json
from prometheus_client.parser import text_string_to_metric_families
from collections import defaultdict

from zmon_worker_monitor.zmon_worker.errors import HttpError, CheckError, ConfigurationError
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from requests.adapters import HTTPAdapter

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

import tokens

# will use OAUTH2_ACCESS_TOKEN_URL environment variable by default
# will try to read application credentials from CREDENTIALS_DIR
tokens.configure()
tokens.manage('uid', ['uid'])
tokens.start()

logger = logging.getLogger('zmon-worker.entity-wrapper')


class EntityWrapperFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(HttpFactory, self).__init__()


    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """

        self.service_url = conf.get('entityservice.url', conf.get('dataservice.url', 'https://localhost:443'))
        self.oauth2 = conf.get('entityservice.oauth2', conf.get('dataservice.oauth2', False))

        return


    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(EntityWrapper, infrastructure_account = factory_ctx.get('infrastructure_account'), service_url = self.service_url, oauth2 = self.oauth2)


class EntityWrapper(object):
    def __init__(self, service_url, infrastructure_account, verify=True, oauth2=False):

        if not url:
            raise ConfigurationError('KairosDB wrapper improperly configured. URL is missing!')

        self.service_url = service_url
        self.__session = requests.Session()

        if oauth2:
            self.__session.headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})

    def _request(q):
        try:
            response = self.__session.get(self.service_url, params={"query": json.dumps(q)})
            if response.ok:
                return response.json()
            else:
                raise Exception(
                    'EntityWrapper query failed: {} with status {}:{}'.format(q, response.status_code, response.text))
        except requests.Timeout:
            raise HttpError('timeout', self.url), None, sys.exc_info()[2]
        except requests.ConnectionError:
            raise HttpError('connection failed', self.url), None, sys.exc_info()[2]

    def search_local(stack_name, type=None, stack_version=None, infrastructure_account=None):
        ia = infrastructure_account if infrastructure_account else self.infrastructure_account
        q = {}

        if type:
            q.update({"type": type})

        if stack_name:
            q.update({"stack_name": stack_name})

        if stack_version:
            q.update({"stack_version": stack_version})

        q.update({"infrastructure_account": infrastructure_account})

        return self._request(q)

    def search_all(stack_name, type, stack_version=None):
        q = {}

        if type:
            q.update({"type": type})

        if stack_name:
            q.update({"stack_name": stack_name})

        if stack_version:
            q.update({"stack_version": stack_version})

        return self._request(q)
