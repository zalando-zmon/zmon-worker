#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import json

from urllib2 import urlparse

import requests
import tokens

from zmon_worker_monitor.zmon_worker.errors import HttpError, ConfigurationError, CheckError
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

# will use OAUTH2_ACCESS_TOKEN_URL environment variable by default
# will try to read application credentials from CREDENTIALS_DIR
tokens.configure()
tokens.manage('uid', ['uid'])
tokens.start()

logger = logging.getLogger('zmon-worker.entities-wrapper')


class EntitiesWrapperFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(EntitiesWrapperFactory, self).__init__()

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

        return propartial(EntitiesWrapper,
                          infrastructure_account=factory_ctx.get('entity', {}).get('infrastructure_account', None),
                          service_url=self.service_url, oauth2=self.oauth2)


class EntitiesWrapper(object):
    def __init__(self, service_url, infrastructure_account, verify=True, oauth2=False):

        if not service_url:
            raise ConfigurationError('EntitiesWrapper improperly configured. URL is missing!')

        self.infrastructure_account = infrastructure_account
        self.__service_url = urlparse.urljoin(service_url, 'api/v1/')
        self.__session = requests.Session()

        self.__session.headers.update({'User-Agent': get_user_agent()})
        self.__session.verify = verify

        if oauth2:
            self.__session.headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})

    def _request(self, endpoint, q, method='get'):
        try:
            url = urlparse.urljoin(self.__service_url, endpoint)

            request = getattr(self.__session, method.lower())

            if method.lower() == 'post':
                response = request(url, json=q)
            else:
                response = request(url, params={'query': json.dumps(q)})

            if response.ok:
                return response.json()
            else:
                raise CheckError(
                    'EntitiesWrapper query failed: {} with status {}:{}'.format(q, response.status_code, response.text))
        except requests.Timeout:
            raise HttpError('timeout', self.__service_url), None, sys.exc_info()[2]
        except requests.ConnectionError:
            raise HttpError('connection failed', self.__service_url), None, sys.exc_info()[2]

    def search_local(self, **kwargs):
        infrastructure_account = kwargs.get('infrastructure_account', self.infrastructure_account)

        q = kwargs
        q.update({'infrastructure_account': infrastructure_account})

        return self._request('entities', q)

    def search_all(self, **kwargs):
        q = kwargs
        return self._request('entities', q)

    def alert_coverage(self, **kwargs):
        infrastructure_account = kwargs.get('infrastructure_account', self.infrastructure_account)

        q = kwargs
        q.update({'infrastructure_account': infrastructure_account})

        return self._request('status/alert-coverage', [q], method='post')


if __name__ == '__main__':
    factory = EntitiesWrapperFactory()
    factory.configure({'entityservice.url': sys.argv[1], 'entityservice.oauth2': True})

    wrapper = factory.create({})

    print wrapper().search_all(stack_name='data-service')
