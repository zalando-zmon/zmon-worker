#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json
from flask import current_app, Blueprint, Response, request, url_for, redirect, jsonify


from zmon_worker_monitor.rpc_client import get_rpc_client_plus


API_VERSION_V1 = 1
API_VERSION = API_VERSION_V1


api_v1_bp = Blueprint('api_v1_bp', __name__)


_rpc_client = None


def get_rpc_client():
    global _rpc_client

    if not _rpc_client:
        _rpc_client = get_rpc_client_plus(current_app.config['RPC_URL'])
    return _rpc_client


def get_logger():
    return logging.getLogger(__name__)


time_conversions = [
    dict(ends=[''], f=lambda x: float(x)),  # conversion if no ending units
    dict(ends=['s', 'sec', 'secs', 'seconds'], f=lambda x: float(x)),  # conversion ending units secs
    dict(ends=['m', 'min', 'mins', 'minutes'], f=lambda x: float(x) * 60),  # conversion ending units min
    dict(ends=['h', 'hour', 'hours'], f=lambda x: float(x) * 3600),  # conversion ending units hours
]


def convert_with_units(val, units=None):
    units = time_conversions if not units else units
    val = str(val)
    for d in units:
        try:
            end = [e for e in d['ends'][::-1] if val.endswith(e)][0]
            return d['f'](val[:len(val) - len(end)])
        except:
            pass
    return None


def error_response(e, status=500, mimetype='application/json'):
    return Response(response=json.dumps({'error': str(e)}), status=status, mimetype=mimetype)


@api_v1_bp.route('/')
def root():
    return 'Hello World!'


@api_v1_bp.route('/procs/')
@api_v1_bp.route('/running_procs/')
@api_v1_bp.route('/list_running/')
@api_v1_bp.route('/running_processes/')
def list_running_redirects():
    return redirect(url_for('.processes_view'))


# TODO: add query parm ?fields=(running,dead)
@api_v1_bp.route('/processes/')
def processes_view():
    return common_rpc_call('processes_view')


@api_v1_bp.route('/processes/<string:proc_id>/')
def single_process_view(proc_id):
    key = request.args.get('key', 'name')
    return common_rpc_call('single_process_view', proc_id, key)


# TODO: change to /status/?interval=1h
@api_v1_bp.route('/status/')
def status():

    default_interval = 60 * 60 * 24 * 365  # TODO: better default for time_window
    interval = request.args.get('interval', default_interval)
    interval = convert_with_units(interval, units=time_conversions)
    if not interval:
        return error_response('Invalid interval {}'.format(request.args.get('interval')))

    return common_rpc_call('status_view', interval=interval)


@api_v1_bp.route('/health')
def health():
    try:
        result = get_rpc_client().health_state()
        resp = jsonify(value=result)
        resp.status_code = 200 if result else 503
    except Exception as e:
        get_logger().exception('Error calling rpc from web_server. Details: ')
        resp = jsonify({'error': str(e)})
        resp.status_code = 500
    return resp


@api_v1_bp.route('/rpc/<rpc_method>/')
def rpc_query(rpc_method=None):
    kwargs = dict(request.args)  # at least can pass strings as kwargs
    return common_rpc_call(rpc_method, **kwargs)


def common_rpc_call(rpc_method, *args, **kwargs):
    try:
        result = get_rpc_client().call_rpc_method(rpc_method, args=args, kwargs=kwargs)
        return Response(response=json.dumps(result), status=200, mimetype='application/json')
    except Exception as e:
        get_logger().exception('Error calling rpc from web_server. Details: ')
        return Response(response=json.dumps({'error': str(e)}), status=500, mimetype='application/json')
