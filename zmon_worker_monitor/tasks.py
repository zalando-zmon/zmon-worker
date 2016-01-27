#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from zmon_worker.tasks.notacelery_task import NotaZmonTask
from zmon_worker.notifications.mail import Mail
from zmon_worker.notifications.sms import Sms


logger = logging.getLogger(__name__)


def configure_tasks(config):
    #Pass configuration to zmon classes
    NotaZmonTask.configure(config)

    Mail.update_config(config)
    Sms.update_config(config)


zmontask = NotaZmonTask()


def check_and_notify(req, alerts, task_context=None, **kwargs):
    logger.debug('check_and_notify received req=%s, alerts=%s, task_context=%s, ', req, alerts, task_context)

    zmontask.check_and_notify(req, alerts, task_context=task_context)


def trial_run(req, alerts, task_context=None, **kwargs):
    logger.info('trial_run received <== check_id=%s', req['check_id'])
    logger.debug('trial_run received req=%s, alerts=%s, task_context=%s, ', req, alerts, task_context)

    zmontask.trial_run(req, alerts, task_context=task_context)


def cleanup(*args, **kwargs):
    logger.info('cleanup task received with args=%s, kwargs=%s', args, kwargs)

    zmontask.cleanup(*args, **kwargs)

