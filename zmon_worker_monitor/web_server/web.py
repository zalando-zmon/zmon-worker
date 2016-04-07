#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Logic for a basic HTTP server (flask app). This code will most likely be run in a separate process.
We expose several HTTP endpoints that return information about the operations of zmon-worker processes.

This app obtain information on the operation of zmon-worker by querying ZMON's internal RPC server.
Said RPC server exposes the methods of ProcessController, our process supervisor, which lives in the
parent process.

"""

import logging
import json
from flask import Flask, Response, request, url_for, redirect, jsonify

from ..rpc_client import get_rpc_client_plus


app = Flask(__name__)


_rpc_endpoint = 'http://localhost:8000/rpc_path'


_rpc_client = None


def get_logger():
    return logging.getLogger(__name__)


@app.route('/')
def root():
    return 'Hello World!'


@app.route('/procs/')
@app.route('/running_procs/')
@app.route('/list_running/')
@app.route('/running_processes/')
def list_running_redirects():
    return redirect(url_for('process_view'))


@app.route('/processes/')
def process_view():
    return common_rpc_call('process_view')


@app.route('/processes/<proc_id>/')
@app.route('/processes/<by>/<proc_id>/')
def single_process_view(proc_id, by='name'):
    by = 'name' if by in ('name', 'proc_name') else 'pid'
    return common_rpc_call('single_process_view', proc_id, by)

@app.route('/status/')
@app.route('/status/time_window/<int:time_window>/')
@app.route('/status/time_window/<float:time_window>/')
@app.route('/status/time_window/<unit>/<int:time_window>/')
@app.route('/status/time_window/<unit>/<float:time_window>/')
def status(unit='sec', time_window=None):
    time_window = time_window if time_window else 60*60*24*365  # TODO: better default for time_window
    time_window = time_window * 3600 if str(unit).lower() in ('hours', 'hour', 'h') else time_window
    return common_rpc_call('status_view', time_window=time_window)


@app.route('/health')
def health():
    try:
        result = _rpc_client.health_state()
        resp = jsonify(value=result)
        resp.status_code = 200 if result else 503
    except Exception as e:
        get_logger().exception('Error calling rpc from web_server. Details: ')
        resp = jsonify({'error': str(e)})
        resp.status_code = 500
    return resp


@app.route('/rpc/<rpc_method>/')
def rpc_query(rpc_method=None):
    kwargs = dict(request.args)  # at least can pass strings as kwargs
    return common_rpc_call(rpc_method, **kwargs)


def common_rpc_call(rpc_method, *args, **kwargs):
    try:
        result = _rpc_client.call_rpc_method(rpc_method, args=args, kwargs=kwargs)
        return Response(response=json.dumps(result), status=200, mimetype='application/json')
    except Exception as e:
        get_logger().exception('Error calling rpc from web_server. Details: ')
        return Response(response=json.dumps({'error': str(e)}), status=500, mimetype='application/json')


def run(listen_on="0.0.0.0", port=8080, threaded=False, rpc_url=None):
    global _rpc_endpoint, _rpc_client

    _rpc_endpoint = rpc_url
    _rpc_client = get_rpc_client_plus(rpc_url)

    logger = get_logger()

    try:
        app.run(host=listen_on, port=port, threaded=threaded, debug=False, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Caught user signal to stop webserver: exiting!")
    except Exception:
        logger.exception("Web server process crashed. Caught exception with details: ")
        raise
