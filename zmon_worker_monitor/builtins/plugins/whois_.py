#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pythonwhois

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


class WhoisFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(WhoisFactory, self).__init__()

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
        return propartial(WhoisWrapper, host=factory_ctx['host'])


class WhoisWrapper(object):

    def __init__(self, host, timeout=10):
        self.host = host
        self.timeout = timeout

    def check(self):
        parsed = {}
        data, server_list = pythonwhois.net.get_whois_raw(self.host, with_server_list=True)
        if len(server_list) > 0:
            parsed = pythonwhois.parse.parse_raw_whois(data, normalized=True, never_query_handles=False,
                                                       handle_server=server_list[-1])
        else:
            parsed = pythonwhois.parse.parse_raw_whois(data, normalized=True)

        return parsed


if __name__ == '__main__':
    import json
    import sys
    import datetime


    def json_fallback(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        else:
            return obj


    data = WhoisWrapper(sys.argv[1]).check()
    print json.dumps(data, default=json_fallback, indent=4)
