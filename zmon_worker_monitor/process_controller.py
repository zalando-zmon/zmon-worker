# -*- coding: utf-8 -*-
"""
Logic for controlling worker processes
"""

import os
import signal
import time
import copy
import json
import pickle
import logging

from multiprocessing import Process
from threading import Thread
from UserDict import IterableUserDict
from collections import defaultdict, Iterable
from functools import wraps
from datetime import timedelta

from .flags import has_flag, flags2num
from .flags import MONITOR_RESTART, MONITOR_KILL_REQ, MONITOR_PING, MONITOR_NONE


FLOAT_DIGITS = 5


class ProcessController(object):
    """
    Class to handle a bunch of child processes
    what can it do:
    0. define a common target function for every process?
    1. spawn N processes that execute the target function, store references to objects and its pid
    2. spawn more process after some are running
    3. terminate some process *(by pid?)
    4. spawn a thread loop for checking the health of child processes *(and take some action if some process dies)?
    5. dynamically change the policy on how to react to process dies *(use queue for incoming requests?)
    """

    # TODO: keep?  ... flags presented also as fields for easier access
    MONITOR_RESTART = MONITOR_RESTART
    MONITOR_PING = MONITOR_PING
    MONITOR_KILL_REQ = MONITOR_KILL_REQ
    MONITOR_NONE = MONITOR_NONE

    def __init__(self, default_target=None, default_args=None, default_kwargs=None, default_flags=None,
                 max_processes=1000, start_action_loop=True):

        self.logger = logging.getLogger(__name__)

        # init default flags
        default_flags = default_flags if default_flags is not None else MONITOR_NONE

        # initializate ProcessGroup
        self.proc_group = ProcessGroup(group_name='main', default_target=default_target, default_args=default_args,
                                       default_kwargs=default_kwargs, default_flags=default_flags,
                                       default_kill_wait=0.5, max_processes=max_processes)

        self.proc_groups = {}  # TODO: allow creation of separated process groups ?

        if start_action_loop:
            self.start_action_loop()

    def create_proc_group(self, name='main', default_target=None, default_args=None, default_kwargs=None,
                          default_flags=None, default_kill_wait=0.5, max_processes=1000):
        # TODO: allow creation of separated process groups ?
        self.proc_groups[name] = ProcessGroup(group_name='main', default_target=default_target,
                                              default_args=default_args, default_kwargs=default_kwargs,
                                              default_flags=default_flags, default_kill_wait=default_kill_wait,
                                              max_processes=max_processes)
        return self.proc_groups[name]

    def spawn_process(self, target=None, args=None, kwargs=None, flags=None):
        return self.proc_group.spawn_process(target=target, args=args, kwargs=kwargs, flags=flags)

    def spawn_many(self, num, target=None, args=None, kwargs=None, flags=None):
        return self.proc_group.spawn_many(num, target=target, args=args, kwargs=kwargs, flags=flags)

    def terminate_process(self, proc_name, kill_wait=None):
        return self.proc_group.terminate_process(proc_name=proc_name, kill_wait=kill_wait)

    def terminate_all_processes(self, kill_wait=None):
        self.proc_group.stop_action_loop()  # stop action loop before starting to terminate child processes
        self.proc_group.terminate_all(kill_wait=kill_wait)
        self.logger.info("proc_stats after terminate_all_processes() : %s", self.proc_group.dead_stats)
        return True

    def list_running(self):
        return [proc.to_dict(serialize_all=True) for proc in self.proc_group.values()]

    def get_info(self, proc_name):
        """
        Get all the info I can of this process, for example:
        1. How long has it been running? *(Do I need an extra pid table for statistics?)
        2. How much memory does it use?
        """
        raise NotImplementedError('Method get_info not implemented yet')

    def list_stats(self):
        return [d['stats'] for d in self.list_running()]

    def is_action_loop_running(self):
        return self.proc_group.is_action_loop_running()

    def mark_for_termination(self, pid):
        self.proc_group.mark_for_termination(pids=[pid])

    def ping(self, pid, data):
        self.proc_group.add_ping(pid, data)

    def add_events(self, pid, events):
        self.proc_group.add_events(pid, events)

    def processes_view(self):
        return self.proc_group.processes_view()

    def single_process_view(self, id, key=None):
        key = str(key).lower()

        proc = None
        if key in ('name', 'proc_name'):
            proc = self.proc_group.get_by_name(id)
        elif key in ('pid', ) and str(id).isdigit():
            proc = self.proc_group.get_by_pid(int(id))

        if not proc:
            return None

        return proc.to_dict(serialize_all=True)

    def status_view(self, interval=None):
        return self.proc_group.status_view(interval=interval)

    def health_state(self):
        return self.proc_group.is_healthy()

    def start_action_loop(self):
        self.proc_group.start_action_loop()

    def stop_action_loop(self):
        self.proc_group.stop_action_loop()


class SimpleMethodCacheInMemory(object):
    """
    Simple cache-like decorator for class methods (receiving self as first argument).
    Do not use it for functions, classmethods or staticmethods.
    We use it mostly for marking methods of ProcessGroup that will run in the action loop in certain intervals
    and for limited caching of some methods without having to add another heavy dependency to the project.
    """

    decorated_functions = defaultdict(set)  # {region => set(func_id1, func_id2, ...)}

    # { region => { class_instance_id => { func_id => { args_key => returned } } } }
    returned = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    # { region => { class_instance_id => { func_id => { args_key => timestamp } } } }
    t_last_exec = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    shortcut_cache = False  # useful to deactivate all cache during testing

    def __init__(self, region='', wait_sec=5, action_flag=None):
        assert '-' not in region, "'-' char not allowed in regions"
        self.region = region
        self.wait_sec = wait_sec
        self.action_flag = action_flag if action_flag else MONITOR_NONE

    @classmethod
    def make_args_key(cls, args, kwargs):
        return '{}-{}'.format(args, sorted((k, v) for k, v in kwargs.items()))

    def __call__(self, f):
        id_f = id(f)
        self.decorated_functions[self.region].add(id_f)

        @wraps(f)
        def wrapper(*args, **kwargs):
            id_class_instance = id(args[0])  # TODO: detect case where f is not bounded to support functions
            args_key = self.make_args_key(args[1:], kwargs)
            t_last = self.t_last_exec[self.region][id_class_instance][id_f].get(args_key, 0)
            if time.time() - t_last >= self.wait_sec or self.shortcut_cache:
                r = f(*args, **kwargs)
                self.returned[self.region][id_class_instance][id_f][args_key] = r
                self.t_last_exec[self.region][id_class_instance][id_f][args_key] = time.time()
                return r
            else:
                return self.returned[self.region][id_class_instance][id_f][args_key]

        wrapper.action_flag = self.action_flag
        wrapper.wrapped_func = f
        return wrapper

    @classmethod
    def get_registered_by_obj(cls, obj, region=''):
        methods = []
        for name in dir(obj):
            f = getattr(obj, name)
            if callable(f) and hasattr(f, 'wrapped_func') and id(getattr(f, 'wrapped_func')) in \
                    cls.decorated_functions.get(region, set()):
                methods.append(f)
        return methods

    @classmethod
    def invalidate(cls, region='', obj=None, method=None):
        assert obj if method else True, 'Need to pass the object the method is bound to'
        if not obj:  # invalidate a whole region
            cls.t_last_exec.pop(region, None)
        elif not method:  # invalidate all methods from an object
            cls.t_last_exec[region].pop(id(obj), None)
        else:  # invalidate just this method
            cls.t_last_exec[region][id(obj)].pop(id(getattr(method, 'wrapped_func')), None)


register = SimpleMethodCacheInMemory
cache = SimpleMethodCacheInMemory


class ProcessPlus(Process):
    """
    A multiprocessing.Process class extended to include all information we attach to the process
    """

    _pack_fields = ('target', 'args', 'kwargs', 'flags', 'tags', 'stats', 'name', 'pid', 'previous_proc',
                    'ping_status', 'actions_last_5', 'errors_last_5', 'task_counts', 'event_counts')

    keep_pings = 3000  # covers approx. 24 hours if pings sent every 30 secs

    keep_events = 200

    initial_wait_pings = 120

    _ping_template = {
        'timestamp': 0,
        'timedelta': 0,
        'tasks_done': 0,
        'percent_idle': 0,
        'task_duration': 0.0,
    }

    default_status_interval = 60 * 5  # analyze only the pings received since now - this interval

    default_ping_count_intervals = (60 * 5, 60 * 30, 60 * 60, 60 * 60 * 6)

    default_event_count_intervals = (60 * 60 * 24, )

    STATUS_OK = 'OK'
    STATUS_OK_IDLE = 'OK-IDLE'
    STATUS_OK_INITIATING = 'OK-INITIATING'
    STATUS_BAD_NO_PINGS = 'BAD-NO-PINGS'
    STATUS_WARN_LONG_TASK = 'WARN_LONG_TASK'
    STATUS_BAD_MALFORMED = 'BAD-MALFORMED-PINGS'
    STATUS_BAD_DEAD = 'BAD-DEAD'
    STATUS_NOT_TRACKED = 'NOT-TRACKED'

    _event_template = {
        'origin': '',
        'type': '',
        'body': '',
        'timestamp': 0,
        'repeats': 0,
    }

    EVENT_TYPE_ACTION = 'ACTION'
    EVENT_TYPE_ERROR = 'ERROR'
    EVENT_TYPE_EXCEPTION = 'EXCEPTION'

    event_types = (EVENT_TYPE_ACTION, EVENT_TYPE_ERROR, EVENT_TYPE_EXCEPTION)

    def __init__(self, target=None, args=(), kwargs=None, flags=None, tags=None, **extra):

        # passed info
        self.target = target if callable(target) else self._str2func(target)
        self.args = args or ()
        self.kwargs = kwargs or {}
        # flags = FLAG_A | FLAG_B | FLAG_X  or  flags = (FLAG_A, FLAG_B, FLAG_X)
        self.flags = flags2num(flags) if isinstance(flags, Iterable) else (flags or MONITOR_NONE)
        self.tags = tags

        # extra info we generate

        self.stats = {
            'stats_closed': False,
            'alive': True,
            'rebel': False,
            'abnormal_termination': False,
            'start_time': None,
            'end_time': None,
            'start_time_str': '',
            'end_time_str': '',
            't_running_secs': 0,
            'exitcode': 0,
            'name': None,
            'pid': None,
        }

        self.stored_pings = []
        self.stored_events = []

        self._rebel = False
        self._termination_mark = False

        # fields that can not be reused in new process (e.g. pid, name).
        self.previous_proc = {
            'dead_name': extra.get('name'),
            'dead_pid': extra.get('pid'),
            'dead_stats': extra.get('stats'),
            'previous_deaths': extra['previous_proc']['previous_deaths'] + 1 if extra.get('previous_proc') else -1,
        }

        self.logger = logging.getLogger(__name__)

        super(ProcessPlus, self).__init__(target=self.target, args=self.args, kwargs=self.kwargs)

    @property
    def abnormal_termination(self):
        return self.stats['abnormal_termination']

    @abnormal_termination.setter
    def abnormal_termination(self, ab_state):
        self.stats['abnormal_termination'] = ab_state

    @property
    def start_time(self):
        return self.stats['start_time']

    @property
    def t_running_secs(self):
        end_time = self.stats['end_time'] or time.time()
        return end_time - self.stats['start_time'] if self.stats['start_time'] else 0

    @property
    def ping_status(self):
        return self.get_ping_status()

    @property
    def actions_last_5(self):
        return self.get_events(event_type=ProcessPlus.EVENT_TYPE_ACTION, limit=5)

    @property
    def errors_last_5(self):
        return self.get_events(event_type=ProcessPlus.EVENT_TYPE_ERROR, limit=5)

    @property
    def exceptions_last_5(self):
        return self.get_events(event_type=ProcessPlus.EVENT_TYPE_EXCEPTION, limit=5)

    @property
    def task_counts(self):
        return self.get_ping_counts()

    @property
    def event_counts(self):
        return self.get_event_counts()

    def is_rebel(self):
        return self._rebel

    def mark_for_termination(self):
        self._termination_mark = True

    def should_terminate(self):
        return self._termination_mark

    def has_flag(self, flag):
        return has_flag(self.flags, flag)

    def add_event_explicit(self, origin, event_type, body, repeats=1):
        event = dict(origin=origin, type=event_type, body=body, repeats=repeats, timestamp=time.time())
        self.add_event(event)

    def add_event(self, data):
        self._assert_valid_event(data)
        self.stored_events.append(data)
        self.stored_events = self.stored_events[-self.keep_events:]

    def get_events(self, event_type=None, interval=None, limit=-1):
        tnow = time.time()
        r = [e for e in self.stored_events if ((not interval or tnow - e['timestamp'] <= interval) and
                                               (not event_type or e['type'] == event_type))]
        return r[-limit:] if limit and limit > 0 else r

    def add_ping(self, data):
        self._assert_valid_ping(data)
        self.stored_pings.append(data)
        self.stored_pings = self.stored_pings[-self.keep_pings:]

    def get_pings(self, interval=None, limit=-1):
        r = self.stored_pings
        if interval is not None:
            tnow = time.time()
            r = [p for p in self.stored_pings if tnow - p['timestamp'] <= interval]
        return r[-limit:] if limit and limit > 0 else r

    def get_ping_status(self, interval=None):

        interval = interval if interval is not None else self.default_status_interval

        if not self.is_alive():
            return self.STATUS_BAD_DEAD

        if not self.has_flag(MONITOR_PING):
            return self.STATUS_NOT_TRACKED

        if self.t_running_secs < self.initial_wait_pings:
            return self.STATUS_OK_INITIATING

        agg_data = self.aggregate_pings(interval=interval)

        if agg_data['tasks_done'] < 0 and agg_data['percent_idle'] < 0:
            return self.STATUS_BAD_NO_PINGS
        if agg_data['tasks_done'] == 0 and agg_data['percent_idle'] < 1:
            return self.STATUS_WARN_LONG_TASK  # this case should self heal, as hard kill should be triggered
        if agg_data['tasks_done'] == 0 and agg_data['percent_idle'] > 99:
            return self.STATUS_OK_IDLE

        return self.STATUS_OK

    @cache(wait_sec=5)
    def aggregate_pings(self, interval=None):

        tnow = time.time()

        if interval is None:  # if time_window not given we aggregate all stored pings
            interval = (tnow - self.stored_pings[0]['timestamp']) if self.stored_pings else 0

        agg_data = {'tasks_per_sec': -1, 'tasks_per_min': -1, 'percent_idle': -1, 'interval': interval,
                    'tasks_done': -1, 'pings_received': -1, 'average_task_duration': 0}

        pings = [p for p in self.stored_pings if tnow - p['timestamp'] <= interval]
        if pings:
            agg_data['tasks_done'] = sum([p['tasks_done'] for p in pings])
            agg_data['tasks_per_sec'] = round(float(agg_data['tasks_done']) / interval, FLOAT_DIGITS)
            agg_data['tasks_per_min'] = round((float(agg_data['tasks_done']) / interval) * 60, FLOAT_DIGITS)
            agg_data['percent_idle'] = round(float(sum([p['percent_idle'] for p in pings])) / len(pings), FLOAT_DIGITS)
            agg_data['pings_received'] = len(pings)
            if agg_data['tasks_done'] > 0:
                agg_data['average_task_duration'] = sum([p['task_duration'] for p in pings]
                                                        ) / float(agg_data['tasks_done'])

        return agg_data

    @cache(wait_sec=5)
    def aggregate_events(self, interval=None):

        def sum_repeats(events):
            return sum([e['repeats'] for e in events])

        def group_by_origin(events):
            events_by_origin = defaultdict(list)
            for e in events:
                events_by_origin[e['origin']].append(e)
            return events_by_origin

        if interval is None:  # if time_window not given we aggregate all stored events
            interval = (time.time() - self.stored_events[0]['timestamp']) if self.stored_events else 0

        all_events = self.get_events(interval=interval)
        actions = self.get_events(event_type=ProcessPlus.EVENT_TYPE_ACTION, interval=interval)
        errors = self.get_events(event_type=ProcessPlus.EVENT_TYPE_ERROR, interval=interval)

        return {
            'interval': interval,
            'totals': {
                'events': sum_repeats(all_events),
                'actions': sum_repeats(actions),
                'errors': sum_repeats(errors),
            },
            'by_origin': {
                'events': {origin: sum_repeats(elist) for origin, elist in group_by_origin(all_events).items()},
                'actions': {origin: sum_repeats(elist) for origin, elist in group_by_origin(actions).items()},
                'errors': {origin: sum_repeats(elist) for origin, elist in group_by_origin(errors).items()},
            },
        }

    @cache(wait_sec=5)
    def get_ping_counts(self, intervals=()):
        intervals = intervals or self.default_ping_count_intervals
        return {str(timedelta(seconds=ts)): self.aggregate_pings(interval=ts) for ts in intervals}

    @cache(wait_sec=5)
    def get_event_counts(self, intervals=()):
        intervals = intervals or self.default_event_count_intervals
        return {str(timedelta(seconds=ts)): self.aggregate_events(interval=ts) for ts in intervals}

    def is_monitored(self):
        return self.has_flag(MONITOR_PING)

    def _assert_valid_event(self, event):
        try:
            assert event['type'] in self.event_types, 'Unrecognized event type: {}'.format(event['type'])
            assert event['repeats'] >= 1, 'Not valid repeat number: Must be greater than 1'
            assert set(event.keys()) == set(self._event_template.keys()), 'Malformed data: {}'.format(event)
            assert not [1 for v in event.values() if v is None], 'event {} with None value is not valid'.format(event)
        except Exception as e:
            self.logger.exception('Bad event: ')
            raise AssertionError(str(e))

    def _assert_valid_ping(self, data):
        try:
            assert set(data.keys()) == set(self._ping_template.keys()), 'Malformed data: {}'.format(data)
        except Exception as e:
            self.logger.exception('Bad ping: ')
            raise AssertionError(str(e))

    def start(self):
        self.stats['start_time'] = time.time()
        self.stats['start_time_str'] = self._time2str(self.stats['start_time'])
        super(ProcessPlus, self).start()

    def terminate_plus(self, kill_wait=0.5):
        success = False
        try:
            if self.is_alive():
                self.logger.info('Terminating process: %s', self.name)
                self.terminate()
                time.sleep(kill_wait)
                if self.is_alive():
                    self.logger.warn('Sending SIGKILL to process with pid=%s', self.pid)
                    os.kill(int(self.pid), signal.SIGKILL)
                    time.sleep(0.1)
                assert not self.is_alive(), 'Fatal: Process {} alive after SIGKILL'.format(self.name)
            else:
                self.logger.warn('Non termination: Process: %s is not alive!', self.name)
                self.abnormal_termination = True
            self.stats = self._closed_stats()
            success = True
        except Exception:
            self.logger.exception('Fatal exception: ')
            self._rebel = True
        return success

    def _updated_stats(self):
        stats = copy.deepcopy(self.stats)
        stats['alive'] = self.is_alive()
        stats['rebel'] = self.is_rebel()
        stats['abnormal_termination'] = self.abnormal_termination
        stats['t_running_secs'] = self.t_running_secs
        stats['name'] = self.name
        stats['pid'] = self.pid
        return stats

    def _closed_stats(self):
        stats = self._updated_stats()
        stats['stats_closed'] = True
        stats['end_time'] = time.time()
        stats['end_time_str'] = self._time2str(stats['end_time'])
        stats['exitcode'] = self.exitcode
        return stats

    @classmethod
    def _time2str(cls, seconds):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(seconds)) if seconds else ''

    @classmethod
    def _func2str(cls, t):
        return pickle.dumps(t, protocol=0) if callable(t) else t

    @classmethod
    def _str2func(cls, str_t):
        return pickle.loads(str_t)

    def to_dict(self, serialize_all=False):
        self.stats = self._updated_stats()
        d = copy.deepcopy({fn: getattr(self, fn) for fn in self._pack_fields})
        if serialize_all:
            d = {fn: (self._func2str(v) if callable(v) else v) for fn, v in d.items()}
        return d

    def to_json(self):
        return json.dumps(self.to_dict(serialize_all=True))

    def __repr__(self):
        return 'ProcessPlus(**{})'.format(self.to_dict(serialize_all=True))

    def __str__(self):
        return self.__repr__()


class ProcessGroup(IterableUserDict):
    """
    Dict-like container of ProcessPlus objects: {process_name => process}
    Perform simple operations on the collection.
    """

    def __init__(self, group_name=None, default_target=None, default_args=None, default_kwargs=None,
                 default_flags=None, default_kill_wait=0.5, max_processes=1000, process_plus_impl=None):

        self.group_name = group_name

        self._defaults = dict(
            target=default_target,
            args=default_args,
            kwargs=default_kwargs,
            flags=self._curate_flags(default_flags),
            kill_wait=default_kill_wait
        )

        self.max_processes = max_processes

        if process_plus_impl:
            assert issubclass(process_plus_impl, ProcessPlus)

        self.ProcessPlusImpl = ProcessPlus if not process_plus_impl else process_plus_impl

        self.__limbo_group = None  # Must access through property
        self.__dead_group = None  # Must access through property

        self._num_keep_dead = 100
        self._num_deleted_dead = 0

        self.logger = logging.getLogger(__name__)

        self._thread_action_loop = None
        self.stop_action = True
        self.action_loop_interval = 1  # seconds between each actions pass

        IterableUserDict.__init__(self)

    def _v_or_def(self, **kw):
        assert len(kw) == 1, 'Wrong call, example of right use: self._val_or_def(kill_wait=10)'
        k, v = kw.keys()[0], kw.values()[0]
        return v if v not in (None, ()) else self._defaults.get(k)

    @classmethod
    def _curate_flags(cls, flags=None):
        return flags2num(flags) if isinstance(flags, Iterable) else (flags or MONITOR_NONE)

    @property
    def limbo_group(self):
        if self.__limbo_group is None:
            self.__limbo_group = ProcessGroup(group_name='limbo')
            self.__limbo_group.stop_action_loop()
        return self.__limbo_group

    @property
    def dead_group(self):
        if self.__dead_group is None:
            self.__dead_group = ProcessGroup(group_name='dead')
            self.__dead_group.stop_action_loop()
        return self.__dead_group

    @property
    def dead_stats(self):
        return [proc.stats for proc in self.dead_group.values()]

    def add(self, process):
        self[process.name] = process

    def spawn_process(self, target=None, args=None, kwargs=None, flags=None, **extra):

        if len(self) >= self.max_processes:
            raise Exception("maximum number of processes reached: {}".format(self.max_processes))

        target = self._v_or_def(target=target)
        args = self._v_or_def(args=args)
        kwargs = self._v_or_def(kwargs=kwargs)
        flags = self._curate_flags(self._v_or_def(flags=flags))

        self.logger.debug('spawning process: target=%s, args=%s, kwargs=%s, flags=%s', repr(target), args, kwargs,
                          flags)
        try:
            proc = self.ProcessPlusImpl(target=target, args=args, kwargs=kwargs, flags=flags, **extra)
            proc.start()
            self.add(proc)
            return proc.name
        except Exception:
            self.logger.exception("Spawn of process failed. Caught exception with details: ")
            raise

    def spawn_many(self, N, target=None, args=None, kwargs=None, flags=None):

        self.logger.debug('spawn_many called with: target=%s, N=%s, args=%s, kwargs=%s, flags=%s', repr(target), N,
                          args, kwargs, flags)
        n_success = 0
        for i in range(N):
            try:
                self.spawn_process(target=target, args=args, kwargs=kwargs, flags=flags)
            except Exception:
                self.logger.exception('Failed to start process. Reason: ')
            else:
                n_success += 1
        return n_success == N  # TODO: better return n_success

    def get_by_pid(self, pid):
        for name, proc in self.items():
            if proc.pid == pid:
                return proc
        self.logger.warn('pid=%s not found in group %s', pid, self.group_name)
        return None

    def get_by_name(self, proc_name):
        proc = self.get(proc_name)
        if not proc:
            self.logger.warn('proc_name=%s not found in group %s', proc_name, self.group_name)
        return proc

    def filtered(self, proc_names=(), pids=(), lambda_proc=None):
        proc_dict = {proc.name: proc for proc in filter(None, map(self.get_by_name, proc_names))}
        proc_dict.update({proc.name: proc for proc in filter(None, map(self.get_by_pid, pids))})
        if lambda_proc and callable(lambda_proc):
            proc_dict.update({proc.name: proc for proc in filter(lambda_proc, self.values())})
        return proc_dict

    def terminate_process(self, proc_name, kill_wait=None):

        kill_wait = self._v_or_def(kill_wait=kill_wait)

        proc = self.pop(proc_name, None)  # pop process out of dict to avoid race conditions with action_loop
        if not proc:
            raise Exception('Process {} not found'.format(proc_name))
        try:
            proc.terminate_plus(kill_wait)
            self.dead_group.add(proc)
        except Exception:
            self.logger.exception('Fatal exception: ')
            # adding proc to limbo to preserve it for second chance to kill
            self.limbo_group.add(proc)
            raise

    def mark_for_termination(self, proc_names=(), pids=()):
        for name, proc in self.filtered(proc_names=proc_names, pids=pids).items():
            proc.mark_for_termination()

    def add_ping(self, pid, data):
        proc = self.get_by_pid(pid)
        if proc:
            proc.add_ping(data)

    def add_events(self, pid, events):
        proc = self.get_by_pid(pid) or self.dead_group.get_by_pid(pid)
        if proc and events:
            for ev in events:
                proc.add_event(ev)

    @cache(wait_sec=30)
    def processes_view(self):
        running_list = []
        dead_list = []

        for name, proc in self.items() + self.dead_group.items():
            plist = running_list if proc.is_alive() else dead_list
            plist.append(proc.to_dict(serialize_all=True))

        return {
            'running': running_list,
            'dead': dead_list,
        }

    @cache(wait_sec=30)
    def status_view(self, interval=None):

        interval = interval or 60 * 5  # TODO add default value in constructor

        total_running_processes = self.total_processes()
        total_dead_processes = self.total_dead_processes()
        total_monitored = self.total_monitored_processes()
        total_tasks_done = 0
        total_tasks_x_sec = 0
        avg_percent_idle = 0

        num_events = 0
        num_actions = 0
        num_errors = 0

        idle_procs = []

        for name, proc in self.items() + self.dead_group.items():
            if proc.ping_status == proc.STATUS_OK_IDLE:  # only alive & monitored procs may have STATUS_OK_IDLE
                idle_procs.append({'name': name, 'pid': proc.pid})

            # aggregations should include alive and dead processes, the inclusion is by time interval
            if proc.is_monitored():
                ping_agg = proc.aggregate_pings(interval=interval)
                total_tasks_done += ping_agg['tasks_done'] if ping_agg['tasks_done'] > 0 else 0
                avg_percent_idle += ping_agg['percent_idle'] if ping_agg['percent_idle'] > 0 else 0

                event_agg = proc.aggregate_events(interval=interval)

                num_events += event_agg['totals']['events']
                num_actions += event_agg['totals']['actions']
                num_errors += event_agg['totals']['errors']

        total_tasks_x_sec = (total_tasks_done * 1.0) / interval if interval > 1e-2 else total_tasks_x_sec
        avg_percent_idle = (avg_percent_idle * 1.0) / total_monitored if total_monitored else 0

        return {
            'idle': {
                'interval': ProcessPlus.default_status_interval,
                'num_idle_procs': len(idle_procs),
                'idle_procs': idle_procs,
            },
            'totals': {
                'interval': interval,
                'total_processes': total_running_processes,
                'total_dead_processes': total_dead_processes,
                'total_monitored_processes': total_monitored,
                'total_unmonitored_processes': total_running_processes - total_monitored,
                'total_tasks_done': total_tasks_done,
                'total_tasks_per_sec': round(total_tasks_x_sec, FLOAT_DIGITS),
                'total_tasks_per_min': round(total_tasks_x_sec * 60, FLOAT_DIGITS),
                'avg_percent_idle': round(avg_percent_idle, FLOAT_DIGITS),
            },
            'events': {
                'interval': interval,
                'num_events': num_events,
                'num_actions': num_actions,
                'num_errors': num_errors,
            },
        }

    def total_processes(self):
        return len(self)

    def total_monitored_processes(self):
        return len([name for name, proc in self.items() if proc.is_monitored()])

    def total_dead_processes(self):
        return len(self.dead_group) + self._num_deleted_dead

    @cache(wait_sec=30)
    def is_healthy(self):
        num_ok, total = 0, 0
        for name, proc in self.items():
            if proc.is_monitored():
                total += 1
                status = proc.get_ping_status()
                if status.startswith('OK'):
                    num_ok += 1
        return num_ok * 2 > total  # current definition: is healthy if half the monitored processes plus one are OK

    def terminate_many(self, proc_names=(), pids=(), kill_wait=None):
        success = True
        for name, proc in self.filtered(proc_names=proc_names, pids=pids).items():
            try:
                self.terminate_process(name, kill_wait=kill_wait)
            except Exception:
                self.logger.exception('Failed to terminate process %s. Reason: ', name)
                success = False
        return success

    def terminate_all(self, kill_wait=None):
        self.terminate_many(proc_names=self.keys(), kill_wait=kill_wait)
        if self.limbo_group:
            limbo_info = [proc.pid for name, proc in self.limbo_group.items()]
            self.logger.error('Fatal: processes left in alive in limbo. PIDs: %s', limbo_info)

    def respawn_process(self, proc_name, kill_wait=None):
        """Terminate process and spawn another process with same arguments"""

        kill_wait = self._v_or_def(kill_wait=kill_wait)

        try:
            proc1 = self.get_by_name(proc_name)
            if not proc1:
                raise Exception('Process {} not found'.format(proc_name))

            was_alive = proc1.is_alive()
            self.terminate_process(proc_name, kill_wait=kill_wait)
            proc2 = self.ProcessPlusImpl(**proc1.to_dict())
            proc2.start()
            self.add(proc2)
            self.logger.debug('Respawned process full details: %s --> New process: %s', proc1, proc2)
            self.logger.warn('Respawned process: proc_name=%s, pid=%s, was_alive=%s --> proc_name=%s, pid=%s',
                             proc1.name, proc1.pid, was_alive, proc2.name, proc2.pid)
            return proc2.name
        except Exception:
            self.logger.exception("Respawn failed. Caught exception with details: ")
            raise

    def get_actions(self):
        return register.get_registered_by_obj(self, region='action')

    @register('action', wait_sec=2)
    def _action_kill_req(self):
        """
        action: respond to kill requests terminate marked pid and spawn them again
        """
        for name, proc in self.items():
            if not self.stop_action and proc.should_terminate() and proc.has_flag(MONITOR_KILL_REQ):
                proc.add_event_explicit('ProcessGroup(%s)._action_kill_req' % self.group_name, 'ACTION',
                                        'Kill request received for %s' % name)
                self.respawn_process(name)

    @register('action', wait_sec=2)
    def _action_restart_dead(self):
        """
        action: inspect all processes and react to those that died unexpectedly
        """
        for name, proc in self.items():
            if not self.stop_action and not proc.is_alive() and proc.has_flag(MONITOR_RESTART):
                msg = 'Detected abnormal termination of pid: %s ... Restarting' % proc.pid
                self.logger.warn(msg)
                proc.add_event_explicit('ProcessGroup(%s)._action_restart_dead' % self.group_name, 'ACTION', msg)
                self.respawn_process(name)

    @register('action', wait_sec=300)
    def _action_clean_limbo(self):
        """
        Clean limbo procs
        """
        for name, proc in self.limbo_group.items():
            if self.stop_action:
                break
            if proc.is_alive():
                self.logger.error('Fatal: process in limbo in undead state!!!!!')
                proc.terminate_plus()
            else:
                self.limbo_group.pop(name, None)
                self.logger.info('Limbo proc was terminated: %s', proc)

    @register('action', wait_sec=600)
    def _action_prune_dead_info(self):
        """
        Remove old stats from dead processes to avoid high memory usage
        """
        num_eliminate = len(self.dead_group) - self._num_keep_dead
        if num_eliminate > 0:
            t_p = sorted([(proc.start_time, name) for name, proc in self.dead_group.items()])
            names_eliminate = [tp[1] for tp in t_p[0:num_eliminate]]
            for name in names_eliminate:
                self.dead_group.pop(name, None)
                self._num_deleted_dead += 1

    def _action_loop(self):
        """
        A threaded loop that runs every interval seconds to perform autonomous actions
        """
        while not self.stop_action:
            for action in self.get_actions():
                if not self.stop_action:
                    try:
                        action()
                    except Exception:
                        self.logger.exception('Error in ProcessController action_loop: ')
            if not self.stop_action:
                time.sleep(self.action_loop_interval)

    def start_action_loop(self, interval=1):
        self.stop_action = False
        self.action_loop_interval = interval  # TODO: better use default value set in constructor ?

        self._thread_action_loop = Thread(target=self._action_loop)
        self._thread_action_loop.daemon = True
        self._thread_action_loop.start()

    def stop_action_loop(self):
        self.stop_action = True

    def is_action_loop_running(self):
        return not self.stop_action
