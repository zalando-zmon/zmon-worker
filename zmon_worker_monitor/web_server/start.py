#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ..settings import LOGGING


def _set_logging(log_conf):
    import logging
    reload(logging)  # prevents process freeze when logging._lock is acquired by the parent process when fork starts
    import logging.config
    logging.config.dictConfig(log_conf)


def start_web(listen_on="0.0.0.0", port=8080, threaded=False, log_conf=None, rpc_url=None):
    """
    Starts HTTP server (flask app). Convenient to use as entry point when starting the server in a child process.
    Notice this server is NOT secure so use it only in a restricted environment.

    :param listen_on: network iface to listen on
    :param port: port to bind to
    :param threaded: use multithreaded server
    :param log_conf: log configuration to use in subprocess in dictConfig format
    :param rpc_url: internal RPC server endpoint
    """

    _set_logging(log_conf if log_conf else LOGGING)

    from . import web  # imported here to avoid flask related modules loaded in workers' memory

    web.run(listen_on=listen_on, port=port, threaded=threaded, rpc_url=rpc_url)
