#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from flask import Blueprint, redirect
from flask_restful import reqparse, Resource
from flask_restful_swagger import swagger
from traceback import format_exc

from .commons import ApiExtended, get_rpc_client
from .errors import ServerError, UserError


API_VERSION_V2 = 2
API_VERSION = API_VERSION_V2


# Create the Api as a blueprint

api_v2_bp = Blueprint('api_v2_bp', __name__)
api_v2 = ApiExtended(api_v2_bp)

# Wrap the Api with swagger docs. Create endpoints: {url_prefix}/spec.html and {url_prefix}/spec.json
api_v2 = swagger.docs(api_v2, apiVersion=API_VERSION, api_spec_url='/spec')


def get_logger():
    return logging.getLogger(__name__)


#
# Api Resources definition
#


class ProcessListApi(Resource):

    @swagger.operation(
        summary='All Processes View',
        notes='Get view of all processes. Notice this returns a big json object.',
        responseMessages=[{'code': 500, 'message': 'System error'}],
    )
    def get(self):

        try:
            client = get_rpc_client()
            r = client.processes_view()
        except Exception as e:
            raise ServerError(message='Error: {}'.format(e), previous_tb=format_exc())

        return r


class ProcessApi(Resource):

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('key', choices=('name', 'proc_name', 'pid'), default='name')
        super(ProcessApi, self).__init__()

    @swagger.operation(
        summary='Get Process by ID',
        notes='Get one process',
        parameters=[
            {
                'name': 'id',
                'description': 'id of the process',
                'required': True,
                'dataType': 'string',
                'paramType': 'path',
            },
            {
                'name': 'key',
                'description': 'key to use to locate the process. Can be name or PID. Defaults to name.',
                'required': False,
                'dataType': 'string',
                'paramType': 'query',
            },
        ],
        responseMessages=[
            {'code': 400, 'message': 'Bad query parameter'},
            {'code': 404, 'message': 'Process with given id not found'},
            {'code': 500, 'message': 'System error'}
        ],
    )
    def get(self, id):

        args = self.parser.parse_args(strict=True)

        try:
            client = get_rpc_client()
            r = client.single_process_view(id, args['key'])
        except Exception as e:
            raise ServerError(code=500, message='Error: {}'.format(e), previous_tb=format_exc())

        if not r:
            raise UserError(code=404, message='Not Found Process by %s=%s' % (args['key'], id))
        return r


class StatusListApi(Resource):

    default_interval = 60 * 5

    units_to_secs = dict(seconds=1, minutes=60, hours=3600, days=3600 * 24)

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('interval', type=float, default=self.default_interval,
                                 help='time interval given in time units (defaults to %s s)' % self.default_interval)
        self.parser.add_argument('units', choices=('seconds', 'minutes', 'hours', 'days'), default='seconds',
                                 help='choices=(seconds, minutes, hours, days). defaults to seconds')
        super(StatusListApi, self).__init__()

    @swagger.operation(
        summary='System Summary View',
        notes='Get status of all processes',
        responseMessages=[{'code': 500, 'message': 'System error'}],
    )
    def get(self):
        args = self.parser.parse_args(strict=True)
        interval = args['interval'] * self.units_to_secs[args['units']]

        try:
            client = get_rpc_client()
            r = client.status_view(interval=interval)
        except Exception as e:
            raise ServerError(code=500, message='Error: {}'.format(e), previous_tb=format_exc())

        return r


class HealthApi(Resource):

    @swagger.operation(
        summary='System Health State',
        notes='Get health <strong>state</strong> of the system',
        responseMessages=[{'code': 503, 'message': 'System in bad health state'}],
    )
    def get(self):

        try:
            client = get_rpc_client()
            value = client.health_state()
            assert value, 'Bad health state'
        except Exception as e:
            raise ServerError(code=503, message='Error in health state: {}'.format(e), previous_tb=format_exc())

        return {'healthy': value}


class Welcome(Resource):

    def get(self):
        return redirect('/spec.html')


#
# Add resources to the Api
#

api_v2.add_resource(Welcome, '/')
api_v2.add_resource(ProcessListApi, '/processes')
api_v2.add_resource(ProcessApi, '/processes/<string:id>')
api_v2.add_resource(StatusListApi, '/status')
api_v2.add_resource(HealthApi, '/health')
