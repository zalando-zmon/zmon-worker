#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Client module for executing RPC methods exposed by a remote server.
This module is compatible with any RPC server, but it is meant to be used to communicate
with ZMON's internal RPC server.

Although RPC specification don't allow calling methods with Python's kwargs, we have extended
this module and ZMON's RPC server to support it. You can pass kwargs to ZMON's RPC server
with one important limitation: kwargs values must be built-in types and json serializable.
In practice this means: nested lists, dicts and all primitive types (int, float, string, ...)

Example of use from code:
  from rpc_client import call_rpc_method
  rpc_url = 'http://localhost:8000/rpc_path'
  result = call_rpc_method(rpc_url, 'my_method', args=[300, 1.1], kwargs={"age": 12, "name": "Peter Pan"})

Another way to do the same:
  from rpc_client import remote_callable
  my_method = remote_callable(rpc_url, 'my_method')
  result = my_method(300, 1.1, age=12, name="Peter Pan")

Same example, but executed from the command line:
  python rpc_client.py http://localhost:8000/rpc_path my_method int:300 float:1.1 'js:{"age": 12, "name": "Peter Pan"}'

"""

import sys
import json
import xmlrpclib


DEBUG = True

_cmd_struct = {
    'endpoint': None,
    'method_name': None,
    'args': []
}


def _serialize_kwargs(kwargs):
    return 'js:{}'.format(json.dumps(kwargs))


def parse_cmd_line(args):
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


def get_rpc_client(endpoint):
    """
    Returns a standard rpc client that connects to the remote server listening at endpoint

    :param str endpoint: RPC url, example http://host:port/rpc_path
    :return: object rpc_client
    """
    return xmlrpclib.ServerProxy(endpoint)


def remote_callable(endpoint, method):
    """
    Returns a callable that represents the remote method.
    Later execute the RPC using callable(*args, **kwargs). See module docstring for limitations on kwargs.

    :param str endpoint: RPC endpoint url
    :param str method: remote method name
    :return: callable representing remote method.
    """

    client = get_rpc_client(endpoint)

    def __wrap_f(*args, **kwargs):
        args = list(args)
        if kwargs:
            args.append(_serialize_kwargs(kwargs))
        return getattr(client, method)(*args)

    return __wrap_f


def call_rpc_method(endpoint, method, args=None, kwargs=None):
    """
    Executes RPC method and returns result.

    :param str endpoint: RPC endpoint url
    :param str method: remote method name
    :param list args: positional arguments to passed
    :param dict kwargs: keyword arguments to passed. See module docstring for limitations.
    :return: result of RPC call
    """

    client = get_rpc_client(endpoint)

    rpc_args = list(args) if args else []
    if kwargs:
        rpc_args.append(_serialize_kwargs(kwargs))

    return getattr(client, method)(*rpc_args)


if __name__ == '__main__':

    if len(sys.argv) <= 2:
        print >> sys.stderr, 'usage: {0} http://<host>:<port>/<rpc_path> <method_name> [ [int|float|str]:arg1 ' \
                             '[int|float|str]:arg2 ...[int|float|str]:argN ...]'.format(sys.argv[0])
        sys.exit(1)

    cmd_line = parse_cmd_line(sys.argv[:])

    if DEBUG:
        print 'Parsed cmd_line: ', cmd_line

    # Executing now the remote method
    result = call_rpc_method(cmd_line['endpoint'], cmd_line['method_name'], *cmd_line['args'])
    if result is not None:
        print ">>Result:\n", result
