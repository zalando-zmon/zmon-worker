# -*- coding: utf-8 -*-
"""
Execution script
"""
import logging
from opentracing_utils import init_opentracing_tracer, trace_requests

trace_requests()  # noqa

import settings


def _set_logging(log_conf):
    import logging
    reload(logging)  # prevents process freeze when logging._lock is acquired by the parent process when fork starts
    import logging.config
    logging.config.dictConfig(log_conf)


def start_worker(**kwargs):
    """
    A simple wrapper for workflow.start_worker(role) , needed to solve the logger import problem with multiprocessing
    :param role: one of the constants workflow.ROLE_...
    :return:
    """
    _set_logging(settings.LOGGING)

    logger = logging.getLogger(__name__)
    logger.info('ZMON Worker running with {} OpenTracing tracer!'.format(kwargs.get('tracer', 'noop')))

    init_opentracing_tracer(kwargs.pop('tracer', None))

    import workflow

    workflow.start_worker_for_queue(**kwargs)
