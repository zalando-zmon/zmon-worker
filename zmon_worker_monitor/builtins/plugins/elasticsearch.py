#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import requests
import sys

import tokens

from zmon_worker_monitor.zmon_worker.errors import HttpError, ConfigurationError
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.elasticsearch-function')


# will use OAUTH2_ACCESS_TOKEN_URL environment variable by default
# will try to read application credentials from CREDENTIALS_DIR
tokens.configure()
tokens.manage('uid', ['uid'])
tokens.start()

DEFAULT_SIZE = 10
MAX_SIZE = 1000
MAX_INDICES = 10

TYPE_SEARCH = '_search'
TYPE_COUNT = '_count'


class ElasticsearchFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(ElasticsearchFactory, self).__init__()

        self._url = None

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        self._url = conf.get('url')

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(ElasticsearchWrapper, url=self._url)


class ElasticsearchWrapper(object):
    def __init__(self, url=None, timeout=10, oauth2=False):
        if not url:
            raise ConfigurationError('Elasticsearch plugin improperly configured. URL is required!')

        self.url = url
        self.timeout = timeout
        self.oauth2 = oauth2
        self._headers = {'User-Agent': get_user_agent()}

    def count(self, indices=None, q='', body=None):
        return self.__query(TYPE_COUNT, indices, q, body, source=False, size=0)

    def search(self, indices=None, q='', body=None, source=True, size=DEFAULT_SIZE):
        return self.__query(TYPE_SEARCH, indices, q, body, source, size)

    def __query(self, query_type, indices, q, body, source, size):
        """
        Search ES cluster using URI search. If ``body`` is None then GET request will be used.

        :param indices: (list) of indices to search. Limited to only 10 indices. ['_all'] will search all available
                        indices, which effectively leads to same results as `None`. Indices can accept wildcard form.
        :param q: (str) of search query.
        :param body: (dict) holding an ES query DSL.
        :param source: (bool) whether to include `_source` field in query response.
        :param size: (int) number of hits to return. Maximum value is 1000. Set to 0 if interested in hits count only.
        :return: ES query result.

        :Example:
        >> es.search(q='http_status:404', source=False)
        >> es.search(indices=['logstash-*'], q='client:192.168.20.* AND http_status:500', size=100, source=False)
        {
            "_shards": {
                "failed": 0,
                "successful": 5,
                "total": 5
            },
            "hits": {
                "hits": [
                    {
                        "_id": "AVSavhqLvZXHftyxi7BG",
                        "_index": "logstash-2016.05.10",
                        "_score": 1.4142135,
                        "_source": {},
                        "_type": "logs"
                    }
                ],
                "max_score": 1.4142135,
                "total": 1
            },
            "timed_out": false,
            "took": 2
        }
        >> es.search(indices=['logstash-*'], q='client:192.168.20.* AND http_status:500', size=0)
        {
            "_shards": {
                "failed": 0,
                "successful": 5,
                "total": 5
            },
            "hits": {
                "hits": [],
                "max_score": 0.0,
                "total": 1
            },
            "timed_out": false,
            "took": 2
        }
        """
        # Sanity checks
        if size < 0 or size > MAX_SIZE:
            raise Exception('Invalid query size. A valid value should be between 0 and {}'.format(MAX_SIZE))

        if indices is not None and type(indices) is not list:
            raise Exception('Invalid indices. A valid value should be None or a list.')

        if type(indices) is list and len(indices) > MAX_INDICES:
            raise Exception('Invalid indices size. Maximum number of indices is {}.'.format(MAX_INDICES))

        indices_str = ''
        if type(indices) is list:
            indices_str = ','.join(indices)

        url = os.path.join(self.url, indices_str, query_type)

        params = {}
        if q:
            params['q'] = q

        if body is None:
            if query_type == TYPE_SEARCH:
                params['size'] = size
                params['_source'] = str(source).lower()
            return self.__request(url, params=params)
        else:
            # Force size limitations in request body.
            if query_type == TYPE_SEARCH:
                body['size'] = size

                if source is False and '_source' not in body:
                    body['_source'] = False

            return self.__request(url, params=params, body=body)

    def health(self):
        """Return ES cluster health."""
        url = os.path.join(self.url, '_cluster', 'health')

        return self.__request(url)

    def __request(self, url, params=None, body=None):
        """Return json response"""
        if self.oauth2:
            self._headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})

        try:
            if body is None:
                response = requests.get(url, params=params, timeout=self.timeout, headers=self._headers)

                if not response.ok:
                    raise Exception('Elasticsearch query failed: {}'.format(url))

                return response.json()
            else:
                response = requests.post(url, params=params, json=body, timeout=self.timeout, headers=self._headers)

                if not response.ok:
                    raise Exception(
                        'Elasticsearch query failed: {} with response: {}'.format(url, json.dumps(response.text)))

                return response.json()
        except requests.Timeout:
            raise HttpError('timeout', self.url), None, sys.exc_info()[2]
        except requests.ConnectionError:
            raise HttpError('connection failed', self.url), None, sys.exc_info()[2]
        except Exception:
            raise


if __name__ == '__main__':
    url = sys.argv[1]
    check = ElasticsearchWrapper(url)

    print(check.search(q='*', source=False))

    print(check.search(size=1, body={
        '_source': False,
        'size': 10000,
        'query': {
            'query_string': {
                'query': '*'
            }
        }
    }))

    print(check.health()['status'])
