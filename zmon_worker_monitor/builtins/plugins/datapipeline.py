#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import requests


from zmon_worker_monitor.zmon_worker.errors import CheckError
from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


logging.getLogger('botocore').setLevel(logging.WARN)


class DataPipelineWrapperFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(DataPipelineWrapperFactory, self).__init__()

    def configure(self, conf):
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(DataPipelineWrapper, region=factory_ctx.get('entity').get('region', None))


def get_region():
    r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=3)
    return r.json()['region']


# create a dict of keys from a list of dicts
def create_dict_from_list_of_fields(fields):
    fields_dict = {}
    for field in fields:
        fields_dict[str(field['key'])] = str(field['stringValue'])

    return fields_dict


class DataPipelineWrapper(object):
    def __init__(self, region=None):
        if not region:
            region = get_region()
        self.__client = boto3.client('datapipeline', region_name=region)

    def get_details(self, pipeline_ids):
        """
            Return a list of pipelines with their details.

            :param name: Pipeline IDs as a String for a single item or list of Strings for multiple pipelines.
            :param type: str
            :param type: list

            :return: Details from the requested pipelines
            :rtype: map
        """
        if isinstance(pipeline_ids, str):
            pipeline_ids = [pipeline_ids]
        else:
            if not isinstance(pipeline_ids, list):
                raise CheckError('Parameter \"pipeline_ids\" should be a string or a list of strings '
                                 'denoting pipeline IDs')

        response = self.__client.describe_pipelines(pipelineIds=pipeline_ids)

        # parse the response and manipulate data to return the pipeline id and its description fields
        pipelines_states = [(str(pipeline['pipelineId']), create_dict_from_list_of_fields(pipeline['fields']))
                            for pipeline in response['pipelineDescriptionList']]
        result = {}

        if not pipelines_states:
            return result

        # create a dict of pipeline_id : details_map
        for (pipeline_id, pipeline_details) in pipelines_states:
            result[pipeline_id] = pipeline_details

        # returns a map which has the pipeline IDs as keys and their details as values
        return result
