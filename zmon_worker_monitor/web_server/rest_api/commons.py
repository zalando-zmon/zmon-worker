#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from flask import current_app
from flask_restful import Api
from .errors import BaseError

from zmon_worker_monitor.rpc_client import get_rpc_client_plus as __get_rpc_client_plus


#
# Extended Api
#


class ApiExtended(Api):
    """
    Extend flask-rest api to handle exceptions in a centralized way
    """
    def handle_error(self, e):
        if isinstance(e, BaseError):
            if e.log_level not in (logging.NOTSET, None):
                logging.getLogger(__name__).log(e.log_level, 'Error in web_server: {}. {}'.format(e, e.previous_tb))
            return self.make_response({'message': e.message}, e.code or 500)

        return super(ApiExtended, self).handle_error(e)  # fall back to flask-restful's error handling


def get_config():
    return current_app.config


#
# RPC related
#

_rpc_client = None


def get_rpc_client():
    global _rpc_client

    if not _rpc_client:
        config = get_config()
        _rpc_client = __get_rpc_client_plus(config['RPC_URL'])
    return _rpc_client
