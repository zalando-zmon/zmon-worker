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

from flask import Flask

_RPC_URL = 'http://localhost:8000/rpc_path'


def get_logger():
    return logging.getLogger(__name__)


def create_app(config):
    app = Flask(__name__)
    app.config.update(config)

    from .rest_api.api_v2 import api_v2_bp

    app.register_blueprint(api_v2_bp, url_prefix='')
    return app


def run(listen_on="0.0.0.0", port=8080, threaded=False, rpc_url=None):

    config = dict(
        DEBUG=False,
        HOST=listen_on,
        PORT=port,
        # URL_PREFIX='/api',
        # My config
        RPC_URL=rpc_url or _RPC_URL,
    )

    logger = logging.getLogger(__name__)
    app = create_app(config)
    try:
        app.run(host=listen_on, port=port, threaded=threaded, debug=False, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Caught user signal to stop webserver: exiting!")
    except Exception:
        logger.exception("Web server process crashed. Caught exception with details: ")
        raise
