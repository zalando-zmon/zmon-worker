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
from flask import Flask, Response, request

from ..rpc_client import call_rpc_method


app = Flask(__name__)


_rpc_endpoint = 'http://localhost:8000/rpc_path'


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/health')
def health():
    return 'Healthy as an ox!'


@app.route('/unhealthy')
def unhealthy():
    return 'UnHealthy as a tardigrad!', 503


@app.route('/rpc/<rpc_method>/')
def rpc_query(rpc_method=None):

    # allowed_methods = ('list_running', 'list_stats')  # TODO: move this to module level ?
    # if rpc_method not in allowed_methods:
    #     return Response(response=json.dumps({'error': 'Not allowed'}), status=403, mimetype='application/json')

    # HTTP GET parameters are used as kwargs to pass to remote procedure
    kwargs = dict(request.args)

    try:
        result = call_rpc_method(_rpc_endpoint, rpc_method, kwargs=kwargs)
        return Response(response=json.dumps(result), status=200, mimetype='application/json')
    except Exception as e:
        return Response(response=json.dumps({'error': str(e)}), status=500, mimetype='application/json')


def run(listen_on="0.0.0.0", port=8080, threaded=False, rpc_url=None):
    global _rpc_endpoint

    _rpc_endpoint = rpc_url

    logger = logging.getLogger(__name__)

    try:
        app.run(host=listen_on, port=port, threaded=threaded, debug=False, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Caught user signal to stop webserver: exiting!")
    except Exception:
        logger.exception("Web server process crashed. Caught exception with details: ")
        raise