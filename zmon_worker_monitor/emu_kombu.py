#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module is intended as a quick and dirty drop in replacement of kombu module, covering only
the functionality we use in zmon.
"""

import re
from collections import namedtuple

Connection = namedtuple('Connection', 'hostname port virtual_host')

def parse_redis_conn(conn_str):
    '''
    Emulates kombu.connection.Connection that we were using only to parse redis connection string
    :param conn_str: example 'redis://localhost:6379/0'
    :return: namedtuple(hostname, port, virtual_host)

    >>> parse_redis_conn('localhost:6379')
    Connection(hostname='localhost', port=6379, virtual_host='0')

    >>> parse_redis_conn('localhost:6379/0')
    Connection(hostname='localhost', port=6379, virtual_host='0')
    '''

    if '://' not in conn_str:
        conn_str = 'redis://' + conn_str

    conn_regex = r'redis://([-.a-zA-Z0-9_]+):([0-9]+)(/[0-9]+)?'
    m = re.match(conn_regex, conn_str)
    if not m:
        raise Exception('unable to parse redis connection string: {}'.format(conn_str))
    return Connection(m.group(1), int(m.group(2)), m.group(3).lstrip('/') if m.group(3) else '0')
