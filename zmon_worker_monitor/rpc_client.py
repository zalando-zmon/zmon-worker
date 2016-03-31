#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module have functions to instantiate RPC clients, to execute RPC methods exposed by a remote server.
You can get a default all-compatible RPC client: client = get_rpc_client(uri)
You can also get an extended rpc client: client = get_rpc_client_plus(uri)

Although RPC specification does not allow calling methods with Python's kwargs, our extended
RPC client supports it, which means you can pass keyword arguments to exposed ZMON RPC methods.
One important limitation: kwargs values must be built-in types and json serializable.
In practice this means: nested lists, dicts, tuples and primitive types (int, float, string)

Example of use from code:
  from rpc_client import get_rpc_client_plus
  client = get_rpc_client_plus('http://localhost:8000/rpc_path')
  result = client.call_rpc_method('my_method', args=[300, 1.1], kwargs={"age": 12, "name": "Peter Pan"})

You can also call the remote method as if it was a member of client:
  client = get_rpc_client_plus('http://localhost:8000/rpc_path')
  result = client.my_method(300, 1.1, age=12, name="Peter Pan")

To execute the same example from the command line:
  python rpc_client.py http://localhost:8000/rpc_path my_method int:300 float:1.1 'js:{"age": 12, "name": "Peter Pan"}'

"""

import sys
import json
import xmlrpclib
from functools import partial


DEBUG = True

_cmd_struct = {
    'endpoint': None,
    'method_name': None,
    'args': []
}


class RpcClientPlus(object):
    """
    A thin wrapper around Python lib rpc client: xmlrpclib.ServerProxy
    It can call RPC methods with keyword arguments (only for ZMON's RPC server).
    Also call_rpc_method(name) is handy for dynamic method name resolution.
    """

    def __init__(self, uri_endpoint, **kwargs):
        self._client = xmlrpclib.ServerProxy(uri_endpoint, **kwargs)

    def _call_rpc_method(self, method, *args, **kwargs):
        rpc_args = list(args)
        if kwargs:
            rpc_args.append(self._serialize_kwargs(kwargs))
        return getattr(self._client, method)(*rpc_args)

    def call_rpc_method(self, method, args=(), kwargs=None):
        """
        Executes RPC method and returns result.

        :param str method: remote method name
        :param list args: positional arguments to passed
        :param dict kwargs: keyword arguments to passed. See module docstring for limitations.
        :return: remote result
        """
        return self._call_rpc_method(method, *(args if args else ()), **(kwargs if kwargs else {}))

    def __getattr__(self, item):
        # you can call remote functions directly like in the original client
        return partial(self._call_rpc_method, item)

    @classmethod
    def _serialize_kwargs(cls, kwargs):
        return 'js:{}'.format(json.dumps(kwargs)) if kwargs else ''


def get_rpc_client(endpoint):
    """
    Returns a standard rpc client that connects to the remote server listening at endpoint

    :param str endpoint: RPC url, example http://host:port/rpc_path
    :return: rpc_client
    """
    return xmlrpclib.ServerProxy(endpoint)


def get_rpc_client_plus(endpoint):
    """
    Returns a extended rpc client that connects to the remote server listening at endpoint

    :param str endpoint: RPC url, example http://host:port/rpc_path
    :return: rpc_client
    """
    return RpcClientPlus(endpoint)


def __parse_cmd_line(args):
    admitted_types = ('int', 'float', 'str')

    cmd_parts = dict(_cmd_struct)
    cmd_parts['endpoint'] = args[1]
    cmd_parts['method_name'] = args[2]
    cmd_parts['args'] = []

    raw_method_args = args[3:]

    for raw_arg in raw_method_args:

        arg_parts = raw_arg.split(':')

        if len(arg_parts) == 1 or arg_parts[0] not in admitted_types:
            arg_type, arg_value = 'str', ':'.join(arg_parts[0:])
            if arg_value.isdigit():
                arg_type = 'int'
            elif not (arg_value.startswith('.') or arg_value.endswith('.')) and arg_value.replace('.', '', 1).isdigit():
                arg_type = 'float'
        else:
            arg_type, arg_value = arg_parts[0], ':'.join(arg_parts[1:])

        try:
            value = eval('{0}({1})'.format(arg_type, arg_value)) if arg_type != 'str' else arg_value
        except Exception:
            print >> sys.stderr, "\n Error: Detected argument with wrong format"
            sys.exit(3)

        cmd_parts['args'].append(value)

    return cmd_parts


if __name__ == '__main__':

    if len(sys.argv) <= 2:
        print >> sys.stderr, 'usage: {0} http://<host>:<port>/<rpc_path> <method_name> [ [int|float|str]:arg1 ' \
                             '[int|float|str]:arg2 ...[int|float|str]:argN ...]'.format(sys.argv[0])
        sys.exit(1)

    cmd_line = __parse_cmd_line(sys.argv[:])

    if DEBUG:
        print 'Parsed cmd_line: ', cmd_line

    # Executing now the remote method
    client = get_rpc_client_plus(cmd_line['endpoint'])
    result = client.call_rpc_method(cmd_line['method_name'], *cmd_line['args'])
    if result is not None:
        print ">>Result:\n", result
