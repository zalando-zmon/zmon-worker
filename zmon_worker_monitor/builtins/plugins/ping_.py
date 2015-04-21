#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


class PingFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(PingFactory, self).__init__()

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(ping, host=factory_ctx['host'])


def ping(host, count=1, timeout=1):
    cmd = [
        'ping',
        '-c',
        str(count),
        '-w',
        str(timeout),
        host,
    ]

    sub = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    sub.communicate()
    ret = sub.wait() == 0
    return ret


if __name__ == '__main__':
    import sys
    print ping(sys.argv[1])
