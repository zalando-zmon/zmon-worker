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


from .consts import has_flag, flags2num
from .consts import MONITOR_RESTART, MONITOR_KILL_REQ, MONITOR_PING, MONITOR_NONE


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

    def status(self):
        return self.proc_group.status()

    def health_state(self):
        return self.proc_group.is_healthy()

    def start_action_loop(self):
        self.proc_group.start_action_loop()

    def stop_action_loop(self):
        self.proc_group.stop_action_loop()


class ProcessPlus(Process):
    """
    A multiprocessing.Process class extended to include all information we attach to the process
    """

    _pack_fields = ('target', 'args', 'kwargs', 'flags', 'tags', 'stats', 'name', 'pid', 'ping_status',
                    'actions_last_5', 'errors_last_5', 'exceptions_last_5', 'status_summary')

    keep_pings = 3000  # covers approx. 24 hours if pings sent every 30 secs

    keep_events = 200

    initial_wait_pings = 120

    _ping_template = {
        'timestamp': 0,
        'timedelta': 0,
        'tasks_done': 0,
        'percert_idle': 0,
    }

    time_window_status = 60 * 5  # analyze only the pings received since now - this interval

    STATUS_OK = 'OK'
    STATUS_OK_IDLE = 'OK-IDLE'
    STATUS_OK_INITIATING = 'OK-INITIATING'
    STATUS_BAD_NO_PINGS = 'BAD-NO-PINGS'
    STATUS_BAD_IS_STUCK = 'BAD-IS-STUCK'
    STATUS_BAD_MALFORMED = 'BAD-MALFORMED-PINGS'
    STATUS_NOT_TRACKED = 'UNKNOWN-NOT-TRACKED'

    _event_template = {
        'origin': '',
        'type': '',
        'body': '',
        'timestamp': 0,
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
        self.flags = flags  # proc_flags = FLAG_A|FLAG_B|FLAG_X
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

        # extra fields can not be reused in new process (e.g. pid, name).
        self._old_name = extra.get('name')
        self._old_pid = extra.get('pid')
        self._old_stats = extra.get('stats')
        self._old_ping_status = extra.get('ping_status')
        self._old_stored_pings = extra.get('stored_pings')
        self._old_stored_pings = extra.get('stored_events')

        self.logger = logging.getLogger(__name__)

        super(ProcessPlus, self).__init__(target=self.target, args=self.args, kwargs=self.kwargs)

    @property
    def abnormal_termination(self):
        return self.stats['abnormal_termination']

    @abnormal_termination.setter
    def abnormal_termination(self, ab_state):
        self.stats['abnormal_termination'] = ab_state

    @property
    def t_running_secs(self):
        end_time = self.stats['end_time'] or time.time()
        return end_time - self.stats['start_time'] if self.stats['start_time'] else 0

    @property
    def ping_avg_5_min(self):
        return self.get_ping_avg(time_window=300)

    @property
    def ping_avg_10_min(self):
        return self.get_ping_avg(time_window=600)

    @property
    def ping_avg_30_min(self):
        return self.get_ping_avg(time_window=1800)

    @property
    def ping_status(self):
        return self.get_ping_status(time_window=self.time_window_status)

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
    def status_summary(self):
        return self.get_status_summary()

    def is_rebel(self):
        return self._rebel

    def mark_for_termination(self):
        self._termination_mark = True

    def should_terminate(self):
        return self._termination_mark

    def has_flag(self, flag):
        return has_flag(self.flags, flag)

    def add_event_explicit(self, origin, event_type, body):
        event = {
            'origin': origin,
            'type': event_type,
            'body': body,
            'timestamp': time.time(),
        }
        self.add_event(event)

    def add_event(self, data):
        self._assert_valid_event(data)
        self.stored_events.append(data)
        self.stored_events = self.stored_events[-self.keep_events:]

    def get_events(self, event_type=None, time_window=None, limit=-1):
        r, tnow = [], time.time()
        for e in self.stored_events:
            if (time_window and tnow - e['timestamp'] > time_window) or (event_type and e['type'] != event_type):
                continue
            r.append(e)
        return r[-limit:] if limit and limit > 0 else r

    def add_ping(self, data):
        self._assert_valid_ping(data)
        self.stored_pings.append(data)
        self.stored_pings = self.stored_pings[-self.keep_pings:]

    def get_pings(self, time_window=None, limit=-1):
        r = self.stored_pings
        if time_window:
            tnow = time.time()
            r = [p for p in self.stored_pings if tnow - p['timestamp'] <= time_window]
        return r[-limit:] if limit and limit > 0 else r

    def get_ping_status(self, time_window=None):

        if not self.has_flag(MONITOR_PING):
            return self.STATUS_NOT_TRACKED

        if not self.stats['start_time'] or self.t_running_secs < self.initial_wait_pings:
            return self.STATUS_OK_INITIATING

        avg_data = self.get_ping_avg(time_window=time_window)

        if avg_data['tasks_done'] < 0 and avg_data['percent_idle'] < 0:
            return self.STATUS_BAD_NO_PINGS
        if avg_data['tasks_done'] < 1 and avg_data['percent_idle'] < 1:
            return self.STATUS_BAD_IS_STUCK  # This shouldn't happen, hard kill should be triggered
        if avg_data['tasks_done'] < 1 and avg_data['percent_idle'] > 98:
            return self.STATUS_OK_IDLE

        return self.STATUS_OK

    def get_ping_avg(self, time_window=None):

        time_window = time_window if time_window else self.time_window_status
        tnow = time.time()

        avg_data = dict(tasks_x_sec=-1, percent_idle=-1, time_window=time_window, tasks_done=-1)

        pings = [p for p in self.stored_pings if tnow - p['timestamp'] <= time_window]
        if pings:
            avg_data['tasks_done'] = sum([p['tasks_done'] for p in pings])
            avg_data['tasks_x_sec'] = float(avg_data['tasks_done']) / time_window
            avg_data['percent_idle'] = float(sum([p['percert_idle'] for p in pings])) / len(pings)

        return avg_data

    def get_status_summary(self, time_window=None):
        time_window = time_window if time_window else self.time_window_status

        return {
            'name': self.name,
            'pid': self.pid,
            'ping_status': self.get_ping_status(time_window=time_window),
            'ping': {
                'ping_avg': self.get_ping_avg(time_window=time_window),
                'ping_avg_5_min': self.get_ping_avg(time_window=300),
                'ping_avg_10_min': self.get_ping_avg(time_window=600),
                'ping_avg_30_min': self.get_ping_avg(time_window=1800),
                'ping_avg_24_h': self.get_ping_avg(time_window=86400),
            },
            'event': {
                'event_summary': self.get_event_summary(time_window=time_window),
                'event_summary_5_min': self.get_event_summary(time_window=300),
                'event_summary_10_min': self.get_event_summary(time_window=600),
                'event_summary_30_min': self.get_event_summary(time_window=1800),
                'event_summary_24_h': self.get_event_summary(time_window=86400),
            },
        }

    def get_event_summary(self, time_window=None):

        if not time_window:
            # if time_window not given we search from the first stored event
            time_window = time.time() - self.stored_events[0]['timestamp'] + 5 if self.stored_events else 0

        all_events = self.get_events(time_window=time_window)
        actions = self.get_events(event_type=ProcessPlus.EVENT_TYPE_ACTION, time_window=time_window)
        errors = self.get_events(event_type=ProcessPlus.EVENT_TYPE_ERROR)
        exceptions = self.get_events(event_type=ProcessPlus.EVENT_TYPE_EXCEPTION)

        events_by_origin = defaultdict(list)
        for e in all_events:
            events_by_origin[e['origin']].append(e)

        return {
            'time_window': time_window,
            'events_total': {
                'num_events': len(all_events),
                'num_actions': len(actions),
                'num_errors': len(errors),
                'num_exceptions': len(exceptions),
            },
            'events_totals_by_origin': {origin: len(elist) for origin, elist in events_by_origin.items()},
        }

    def is_monitored(self):
        return self.has_flag(MONITOR_PING)

    def _assert_valid_event(self, event):
        try:
            assert event['type'] in self.event_types, 'Unrecognized event type: {}'.format(event['type'])
            assert set(event.keys()) == set(self._event_template.keys()), 'Malformed data: {}'.format(event)
            # assert not [1 for v in event.values() if v is None]
        except:
            self.logger.exception('Bad event: ')
            raise

    def _assert_valid_ping(self, data):
        try:
            assert set(data.keys()) == set(self._ping_template.keys()), 'Malformed data: {}'.format(data)
        except:
            self.logger.exception('Bad ping: ')
            raise

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


class SimpleMethodCacheInMemory(object):
    """
    Simple cache-like decorator to mark methods of ProcessGroup that will run in the action loop in certain intervals
    """

    decorated_functions = defaultdict(set)  # {region => set(func_id1, func_id2, ...)}
    returned = {}  # {cache_key => returned}
    t_last_exec = {}  # {cache_key => time }

    def __init__(self, region='', wait_sec=5, action_flag=None):
        self.region = region
        self.wait_sec = wait_sec
        self.action_flag = action_flag if action_flag else MONITOR_NONE

    @classmethod
    def make_key(cls, region, self_received, func, args, kwargs):
        return '{}-{}-{}-{}-{}'.format(region, id(self_received), id(func), args, sorted((k, v) for k, v in
                                                                                         kwargs.items()))

    def __call__(self, f):
        self.decorated_functions[self.region].add(id(f))

        @wraps(f)
        def wrapper(*args, **kwargs):

            class_instance = args[0]  # TODO: detect case where f is not bounded to support functions

            key = self.make_key(self.region, class_instance, f, args[1:], kwargs)

            t_last = self.t_last_exec.get(key)

            if not t_last or (time.time() - t_last >= self.wait_sec):
                r = f(*args, **kwargs)
                self.returned[key] = r
                self.t_last_exec[key] = time.time()
                return r
            else:
                return self.returned[key]

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


register = SimpleMethodCacheInMemory


class ProcessGroup(IterableUserDict):
    """
    Dict-like container of ProcessPlus objects: {process_name => process}
    Perform simple operations on the collection.
    """

    def __init__(self, group_name=None, default_target=None, default_args=None, default_kwargs=None,
                 default_flags=None, default_kill_wait=0.5, max_processes=1000):

        self.group_name = group_name

        self._defaults = dict(
            target=default_target,
            args=default_args,
            kwargs=default_kwargs,
            flags=self._curate_flags(default_flags),
            kill_wait=default_kill_wait
        )

        self.max_processes = max_processes

        self.limbo_data = {}
        self.dead_stats = []
        self.dead_repr = []

        self._num_keep_dead = 1000

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
            proc = ProcessPlus(target=target, args=args, kwargs=kwargs, flags=flags, **extra)
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
        # TODO: cache {pid => proc_name}? PIDs reused slowly in linux (PIDs wrap around ~32768)
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
            self.dead_stats.append(dict(proc.stats))  # store stats
            self.dead_repr.append(repr(proc))  # store representation
        except Exception:
            self.logger.exception('Fatal exception: ')
            # adding proc to limbo to preserve it for second chance to kill
            self.limbo_data[proc_name] = proc
            raise

    def mark_for_termination(self, proc_names=(), pids=()):
        for name, proc in self.filtered(proc_names=proc_names, pids=pids).items():
            proc.mark_for_termination()

    def add_ping(self, pid, data):
        proc = self.get_by_pid(pid)
        if proc:
            proc.add_ping(data)

    def status(self, time_window=None):
        # TODO: add aggregated events

        time_window = time_window or 60 * 5  # TODO add default value in constructor ?

        total_processes = self.total_processes()
        total_monitored = 0
        total_tasks_done = 0
        total_tasks_x_sec = 0
        avg_percent_idle = 0

        status_summary_list = []

        for name, proc in self.items():
            if proc.is_monitored():
                total_monitored += 1
                status_summary = proc.get_status_summary(time_window=time_window)
                ping_avg = status_summary['ping']['ping_avg']
                status_summary_list.append(status_summary)
                total_tasks_done += ping_avg['tasks_done'] if ping_avg['tasks_done'] > 0 else 0
                avg_percent_idle += ping_avg['percent_idle'] if ping_avg['percent_idle'] > 0 else 0

        total_tasks_x_sec = (total_tasks_done * 1.0) / time_window if time_window > 1e-2 else total_tasks_x_sec
        avg_percent_idle = int(round((avg_percent_idle * 1.0) / total_monitored)) if total_monitored else 0

        return {
            'running': status_summary_list,
            'totals': {
                'time_window': time_window,
                'total_processes': total_processes,
                'total_monitored': total_monitored,
                'total_unmonitored': total_processes - total_monitored,
                'total_tasks_done': total_tasks_done,
                'total_tasks_x_sec': total_tasks_x_sec,
                'avg_percent_idle': avg_percent_idle,
            },
            'events': {

            },
        }

    def total_processes(self):
        return len(self)

    def total_monitored_processes(self):
        return len([name for name, proc in self.items() if proc.is_monitored()])

    def is_healthy(self):
        num_ok, total = 0, 0
        for name, proc in self.items():
            status = proc.get_ping_status()
            if status != ProcessPlus.STATUS_NOT_TRACKED:
                total += 1
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
        if self.limbo_data:
            limbo_info = [str(proc) for name, proc in self.limbo_data.items()]
            self.logger.error('Fatal: processes left in alive in limbo %s', limbo_info)

    def respawn_process(self, proc_name, kill_wait=None):
        """Terminate process and spawn another process with same arguments"""

        kill_wait = self._v_or_def(kill_wait=kill_wait)

        try:
            proc1 = self.get_by_name(proc_name)
            if not proc1:
                raise Exception('Process {} not found'.format(proc_name))

            was_alive = proc1.is_alive()
            self.terminate_process(proc_name, kill_wait=kill_wait)
            proc2 = ProcessPlus(**proc1.to_dict())
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
                self.respawn_process(name)

    @register('action', wait_sec=2)
    def _action_restart_dead(self):
        """
        action: inspect all processes and react to those that died unexpectedly
        """
        for name, proc in self.items():
            if not self.stop_action and not proc.is_alive() and proc.has_flag(MONITOR_RESTART):
                self.logger.warn('Detected abnormal termination of pid: %s ... Attempting restart', proc.pid)
                self.respawn_process(name)

    @register('action', wait_sec=300)
    def _action_clean_limbo(self):
        """
        Clean limbo procs
        """
        for name, proc in self.limbo_data.items():
            if self.stop_action:
                break
            if proc.is_alive():
                self.logger.error('Fatal: process in limbo in undead state!!!!!')
                proc.terminate_plus()
            else:
                self.limbo_data.pop(name, None)
                self.logger.info('Limbo proc was terminated: %s', proc)

    @register('action', wait_sec=600)
    def _action_prune_dead_info(self):
        """
        Remove old stats from dead processes to avoid high memory usage
        """
        self.dead_repr = self.dead_repr[-self._num_keep_dead:]
        self.dead_stats = self.dead_stats[-self._num_keep_dead:]

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
