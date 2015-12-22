# -*- coding: utf-8 -*-
"""
Execution script
"""

import settings


def _set_logging(log_conf):
    import logging.config
    logging.config.dictConfig(log_conf)


def start_worker(**kwargs):
    """
    A simple wrapper for workflow.start_worker(role) , needed to solve the logger import problem with multiprocessing
    :param role: one of the constants workflow.ROLE_...
    :return:
    """
    _set_logging(settings.LOGGING)

    import workflow

    workflow.start_worker_for_queue(**kwargs)
