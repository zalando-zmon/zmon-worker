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

    def start_action_loop(self):
        self.proc_group.start_action_loop()

    def stop_action_loop(self):
        self.proc_group.stop_action_loop()


class ProcessPlus(Process):
    """
    A multiprocessing.Process class extended to include all information we attach to the process
    """

    _pack_fields = ('target', 'args', 'kwargs', 'flags', 'tags', 'stats', 'stored_pings', 'name', 'pid')

    keep_pings = 30

    _ping_template = {
        'timestamp': 0,
        'timedelta': 0,
        'tasks_done': 0,
        'percert_idle': 0,
    }

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

        self._rebel = False
        self._termination_mark = False

        # extra fields can not be reused in new process (e.g. pid, name).
        self._old_name = extra.get('name')
        self._old_pid = extra.get('pid')
        self._old_stats = extra.get('stats')
        self._old_stored_pings = extra.get('stored_pings')

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
        return end_time - self.stats['start_time']

    def is_rebel(self):
        return self._rebel

    def mark_for_termination(self):
        self._termination_mark = True

    def should_terminate(self):
        return self._termination_mark

    def has_flag(self, flag):
        return has_flag(self.flags, flag)

    def add_ping(self, data):
        self.stored_pings.append(data)
        self.stored_pings = self.stored_pings[-self.keep_pings:]

    def get_pings(self):
        return self.stored_pings

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


class ProcAction(object):
    """
    Simple cache-like decorator to mark methods of ProcessGroup that will run in the action loop in certain intervals
    """

    decorated_functions = set()  # set(func_id1, func_id2)
    returned = defaultdict(dict)  # {class_instance_id => {func_id => returned}}
    t_last_exec = defaultdict(dict)  # {class_instance_id => {func_id => time_in_secs}}

    def __init__(self, wait_sec=5, action_flag=None):
        self.wait_sec = wait_sec
        self.action_flag = action_flag if action_flag else MONITOR_NONE

    def __call__(self, f):
        f_id = id(f)
        self.decorated_functions.add(f_id)

        @wraps(f)
        def wrapper(*args, **kwargs):
            class_instance = id(args[0])
            t_last = self.t_last_exec[class_instance].get(f_id)

            if not t_last or (time.time() - t_last >= self.wait_sec):
                self.t_last_exec[class_instance][f_id] = time.time()
                r = f(*args, **kwargs)
                self.returned[class_instance][f_id] = r
                return r
            else:
                return self.returned[class_instance][f_id]

        wrapper.action_flag = self.action_flag
        wrapper.wrapped_func = f
        return wrapper

    @classmethod
    def get_registered_by_obj(cls, obj):
        methods = []
        for name in dir(obj):
            f = getattr(obj, name)
            if callable(f) and hasattr(f, 'wrapped_func') and id(getattr(f, 'wrapped_func')) in cls.decorated_functions:
                methods.append(f)
        return methods


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
        return ProcAction.get_registered_by_obj(self)

    @ProcAction(wait_sec=2)
    def _action_kill_req(self):
        """
        action: respond to kill requests terminate marked pid and spawn them again
        """
        for name, proc in self.items():
            if not self.stop_action and proc.should_terminate() and proc.has_flag(MONITOR_KILL_REQ):
                self.respawn_process(name)

    @ProcAction(wait_sec=2)
    def _action_restart_dead(self):
        """
        action: inspect all processes and react to those that died unexpectedly
        """
        for name, proc in self.items():
            if not self.stop_action and not proc.is_alive() and proc.has_flag(MONITOR_RESTART):
                self.logger.warn('Detected abnormal termination of pid: %s ... Attempting restart', proc.pid)
                self.respawn_process(name)

    @ProcAction(wait_sec=300)
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

    @ProcAction(wait_sec=600)
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
