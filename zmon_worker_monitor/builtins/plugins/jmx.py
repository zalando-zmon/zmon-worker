#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import requests
import time

from zmon_worker_monitor.zmon_worker.errors import JmxQueryError, ConfigurationError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


class JmxFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(JmxFactory, self).__init__()
        self._jmxquery_host = None
        self._jmxquery_port = None

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        self._jmxquery_host = conf['jmxquery.host']
        self._jmxquery_port = conf['jmxquery.port']

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(JmxWrapper,
                          jmxqueryhost=self._jmxquery_host,
                          jmxqueryport=self._jmxquery_port,
                          host=factory_ctx['host'],
                          port=factory_ctx['jmx_port'])


class JmxWrapper(object):
    def __init__(self, jmxqueryhost, jmxqueryport, host, port, timeout=5):
        if not jmxqueryhost or not jmxqueryport:
            raise ConfigurationError('JMX wrapper improperly configured. Missing jmxqueryhost & jmxqueryport.')

        # jmxquery running where?
        self.jmxquery_host = jmxqueryhost
        self.jmxquery_port = jmxqueryport

        self.host = host
        self.port = port

        self.timeout = timeout
        self._queries = []

    @staticmethod
    def _transform_results(data):
        '''Transform JSON returned from JMX Query to a reasonable dict
        >>> JmxWrapper._transform_results({'results':[{'beanName':'mybean','attributes':{'HeapMemoryUsage':1}}]})
        {'HeapMemoryUsage': 1}
        >>> res = JmxWrapper._transform_results({'results':[{'beanName':'a','attributes':{'x':1}}, {'beanName': 'b', 'attributes': {'y': 2}}]})
        >>> assert {'a': {'x': 1}, 'b': {'y': 2}} == res
        >>> JmxWrapper._transform_results({'results':[{'beanName':'a','attributes':{'x':{'compositeType': {}, 'contents': {'y':7}}}}]})
        {'x': {'y': 7}}
        '''  # noqa

        results = data['results']
        d = {}
        for result in results:
            attr = result['attributes']

            for key, val in attr.items():
                if 'password' in key.lower():
                    attr[key] = ''
                    continue

                if isinstance(val, dict) and 'compositeType' in val and 'contents' in val:
                    # special unpacking of JMX CompositeType objects (e.g. "HeapMemoryUsage")
                    # we do not want all the CompositeType meta information => just return the actual values
                    attr[key] = val['contents']
            d[result['beanName']] = attr
        if len(d) == 1:
            # strip the top-level "bean name" keys
            return d.values()[0]
        else:
            return d

    def query(self, bean, *attributes):
        self._queries.append((bean, attributes))
        return self

    def _jmxquery_queries(self):
        for bean, attributes in self._queries:
            query = bean
            if attributes:
                query += '@' + ','.join(attributes)
            yield query

    def results(self):
        if not self._queries:
            raise ValueError('No query to execute')

        try:
            r = requests.get('http://{}:{}'.format(self.jmxquery_host, self.jmxquery_port),
                             params={'host': self.host, 'port': self.port,
                                     'query': self._jmxquery_queries()}, timeout=2)

            if r.status_code == 500:
                raise Exception('-do-one-try-')
        except Exception:
            time.sleep(1)
            r = requests.get('http://{}:{}'.format(self.jmxquery_host, self.jmxquery_port),
                             params={'host': self.host, 'port': self.port,
                                     'query': self._jmxquery_queries()}, timeout=2)

        output = r.text

        try:
            data = json.loads(output)
        except Exception:
            raise JmxQueryError(output)

        return self._transform_results(data)


if __name__ == '__main__':
    # example call:
    # JAVA_HOME=/opt/jdk1.7.0_21/ python jmx.py restsn03 49600 jmxremote.password java.lang:type=Memory HeapMemoryUsage
    import sys

    jmx = JmxWrapper(*sys.argv[1:4])
    print jmx.query(*sys.argv[4:]).results()
