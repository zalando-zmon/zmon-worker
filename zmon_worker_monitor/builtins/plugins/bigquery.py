#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

from zmon_worker_monitor.zmon_worker.errors import ConfigurationError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


logger = logging.getLogger('zmon-worker.bigquery-function')


class BigqueryWrapperFunction(IFunctionFactoryPlugin):
    def __init__(self):
        super(BigqueryWrapperFunction, self).__init__()

    def configure(self, conf):
        self._bigquery_key = conf.get('bigquery_key', '')
        self._location = conf.get('location')
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(BigqueryWrapper, bigquery_key=self._bigquery_key, location=self._location)


class BigqueryWrapper(object):
    def __init__(self, bigquery_key, location='EU'):
        self._location = location

        if not bigquery_key:
            raise ConfigurationError('Bigquery key (bigquery_key) is not set.')
        self._bigquery_key = bigquery_key

    def query(self, query_string):
        """
        Query BigQuery and wait for the result to be returned.
        :param query_string: (string) required, raw string contains the BigQuery query
        :return: (dict) the query result
        """
        credentials = service_account.Credentials.from_service_account_info(json.loads(self._bigquery_key))
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        query_job = client.query(query_string, location=self._location)

        return query_job.result()  # Waits for the query to finish


if __name__ == '__main__':
    import os

    bigqueryWrapper = BigqueryWrapper(bigquery_key=os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    query = """SELECT 1 + 1 as value"""
    results = bigqueryWrapper.query(query_string=query)
    for row in results:
        print("{}".format(row))
