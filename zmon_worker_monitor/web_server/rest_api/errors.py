#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging


class BaseError(Exception):
    """
    Base class exception for our handled errors
    """
    def __init__(self, message='', code=500, log='ERROR', previous_tb=''):
        self.message = message
        self.code = code
        self.log_level = log if isinstance(log, int) else (getattr(logging, log) if log else logging.NOTSET)
        self.previous_tb = previous_tb

    def __str__(self):
        return 'Exception.message={}, Exception.code={}'.format(self.message, self.code)


class ServerError(BaseError):
    pass


class UserError(BaseError):
    def __init__(self, message='', code=404, log='INFO', previous_tb=''):
        super(UserError, self).__init__(message=message, code=code, log=log, previous_tb=previous_tb)
