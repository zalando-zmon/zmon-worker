#!/usr/bin/env python
# -*- coding: utf-8 -*-

from emu_kombu import parse_redis_conn
import redis
import logging
import time
from threading import local as thread_local
import collections
import math
from traceback import format_exception


logger = logging.getLogger(__name__)


WAIT_RECONNECT_MIN = 0.1
WAIT_RECONNECT_MAX = 20


class _ThreadLocal(thread_local):
    can_init = False
    instance = None


class RedisConnHandler(object):

    """
    This is a connection manager for redis implemented as a context handler. When used inside a with statement
    it intercepts RedisConnection exceptions as well as its own IdleLoopException in order to keep score of
    failures and idle cycles. Based in this counters it reacts to connection errors by introducing small exponential
    time delays and making several attempts to regain the connection, if t_wait_per_server seconds pass without
    success it switches to the next redis server from the list it given when configured.
    It also switches to the next server after t_wait_no_tasks seconds without getting any task.
    You also get thread safety (connections are not shared among threads), and an easy way to get the connection
    without passing the reference around.
    """

    # Constants
    __CONS_SUPPRESS_EXCEPTION = True

    __CONS_PROPAGATE_EXCEPTION = False

    STATUS_ERROR = 'STATUS_ERROR'
    STATUS_IDLE = 'STATUS_IDLE'
    STATUS_OK = 'STATUS_OK'

    # class variables
    servers = []

    t_wait0 = WAIT_RECONNECT_MIN

    retries_per_server = 5

    t_wait_per_server = 30  # if 30 seconds pass and we have connection errors we switch server

    t_wait_no_tasks = 5 * 60  # if 5 minutes pass without getting any message we switch server

    _max_wait_step = 15  # a top value for our exponential increase in waiting time

    _thread_local = _ThreadLocal()

    # Counters and dates markers for connection errors
    _status = None
    _active_index = 0
    _retries_count = -1
    _idle_count = -1
    message_count = 0
    _last_failure_tstamp = 0
    _last_success_tstamp = time.time()
    _last_message_tstamp = time.time()

    class IdleLoopException(Exception):
        pass

    @classmethod
    def configure(cls, **config):

        cls.t_wait0 = float(config.get('t_wait0', cls.t_wait0))

        cls.t_wait_no_tasks = float(config.get('t_wait_no_tasks', cls.t_wait_no_tasks))

        cls.t_wait_per_server = float(config.get('t_wait_per_server', cls.t_wait_per_server))

        # estimate the retries_per_server from the wait_time_per_server
        cls.retries_per_server = cls.calculate_retries_per_server(cls.t_wait_per_server, cls.t_wait0)

        servers = config.get('redis.servers')
        if servers:
            if isinstance(servers, basestring):
                servers = [s.strip() for s in servers.split(',')]
            elif not isinstance(servers, collections.Iterable):
                raise Exception("wrong servers parameter")
            cls.servers = list(servers)
            # parse all server urls to detect config errors beforehand
            [parse_redis_conn(s) for s in cls.servers]

        logger.info('Configured RedisConnHandler with retries_per_server(estimated)=%s, t_wait_per_server=%s, '
                    't_wait_no_tasks=%s, servers=%s', cls.retries_per_server, cls.t_wait_per_server,
                    cls.t_wait_no_tasks, cls.servers)

    def __init__(self):
        # we could use a metaclass or some trick on __new__ for enforcing the use of get_instance()
        if not self._thread_local.can_init:
            raise AssertionError('You must use get_instance() to get an instance')

        assert len(self.servers) > 0, 'Fatal Error: No servers have been configured'
        self._conn = None
        self._parsed_redis = parse_redis_conn(self.servers[self._active_index])

    @classmethod
    def get_instance(cls):
        if cls._thread_local.instance is None:
            cls._thread_local.can_init = True
            cls._thread_local.instance = cls()
            cls._thread_local.can_init = False
        return cls._thread_local.instance

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        if exc_type is not None:
            if issubclass(exc_type, redis.ConnectionError):
                self.mark(self.STATUS_ERROR)
                logger.error('Lost connection to Redis server: %s. Waiting %s seconds. '
                             'Exception detail follows:\n%s',
                             self.get_active_server(), self.get_wait_time(),
                             ''.join(format_exception(exc_type, exc_val, exc_tb)))
                self.wait_on_error()
                return self.__CONS_PROPAGATE_EXCEPTION

            if issubclass(exc_type, self.IdleLoopException):
                self.mark(self.STATUS_IDLE)
                logger.debug("Idle: %s, message_count: %s", exc_val, self.get_message_count())
                return self.__CONS_SUPPRESS_EXCEPTION

        self.mark(self.STATUS_OK)
        return self.__CONS_PROPAGATE_EXCEPTION

    @staticmethod
    def calculate_wait_time_per_server(retries_per_server, t_wait0):
        return t_wait0 * (2**(retries_per_server + 1) - 1)

    @staticmethod
    def calculate_retries_per_server(wait_time_per_server, t_wait0):
        return int(round(math.log(wait_time_per_server * 1.0 / t_wait0 + 1, 2) - 1))

    def get_active_server(self):
        if self.should_switch_server():
            self.switch_active_server()
        return self.servers[self._active_index]

    def get_parsed_redis(self):
        return self._parsed_redis

    def should_switch_server(self):
        cur_time = time.time()
        return (self.is_previous_error() and cur_time - self._last_success_tstamp > self.t_wait_per_server) or \
               (self.is_previous_idle() and cur_time - self._last_message_tstamp > self.t_wait_no_tasks)

    def is_previous_ok(self):
        return self._retries_count == -1

    def is_previous_error(self):
        return self._retries_count > -1

    def is_previous_idle(self):
        return self._idle_count > -1

    def switch_active_server(self, force_master=False):
        self._active_index = (0 if force_master or self._active_index >= len(self.servers) - 1 else
                              self._active_index + 1)
        self._parsed_redis = parse_redis_conn(self.servers[self._active_index])

        prev_status = self._status
        self.mark(self.STATUS_OK)  # mark a fresh status OK for the new server

        logger.warn('Switched active Redis server to %s, prev_status=%s, force_master=%s', self.servers[self._active_index], prev_status, force_master)

    def get_wait_time(self):
        return min(self.t_wait0 * (2 ** self._retries_count) if self._retries_count >= 0 and not
                   self.should_switch_server() else 0, self._max_wait_step)

    def get_message_count(self):
        return self.message_count

    def wait_on_error(self):
        time.sleep(self.get_wait_time())

    def mark(self, status):
        self._status = status
        if status == self.STATUS_ERROR:
            self._retries_count += 1
            self._last_failure_tstamp = time.time()
            self._idle_count = -1
            self._conn = None  # force the recreation of the thread local connection in any case

        elif status == self.STATUS_IDLE:
            self._idle_count += 1
            self._retries_count = -1  # and idle loop is still a success, so clear previous errors
            self._last_success_tstamp = time.time()

        elif status == self.STATUS_OK:
            self._retries_count = -1
            self._idle_count = -1
            self._last_success_tstamp = time.time()
            self.message_count += 1
            self._last_message_tstamp = time.time()
        else:
            raise Exception('Non valid status: {}'.format(status))

    def get_healthy_conn(self):
        return self.get_conn()

    def get_conn(self):
        if self._conn is not None and not self.should_switch_server():
            return self._conn
        else:
            self._conn = None
            active_server = self.get_active_server()
            c = parse_redis_conn(active_server)
            logger.info('Opening new Redis connection to %s:%s/%s..', c.hostname, c.port, c.virtual_host)
            self._conn = redis.StrictRedis(host=c.hostname, port=c.port, db=c.virtual_host)
            return self._conn
