
import logging
import pytest
import time
# from mock import MagicMock

# import zmon_worker_monitor
from zmon_worker_monitor import process_controller
from zmon_worker_monitor.flags import MONITOR_RESTART, MONITOR_NONE, MONITOR_PING, MONITOR_KILL_REQ


# send all log messages to stdout
logging.basicConfig(level=logging.DEBUG)


def test_action_decorator(monkeypatch):
    """
    Test zmon_worker_monitor.process_controller.ProcAction
    Check1: methods are re-executed only after the waiting time specified
    Check2: different object instances call their respective bounded methods
    """

    # some values used in the checks
    t_wait_method_1 = 0.1
    t_wait_method_2 = 0.2
    some_flag = 16

    class FibClass(object):
        """
        Object that produces an endless stream of possibly repeated numbers from the fibonacci series.
        Numbers increase only after wait_sec, as defined with the ProcAction decorator
        """

        def __init__(self):
            self.x0, self.x1 = -1, 1
            self.state = 0

        @process_controller.register(wait_sec=t_wait_method_1, action_flag=some_flag)
        def next_fibo(self):
            self.x1, self.x0 = self.x1 + self.x0, self.x1
            # check the decorator makes action_flag available inside
            assert self.next_fibo.action_flag == some_flag
            return self.x1

        @process_controller.register(wait_sec=t_wait_method_2)
        def message(self):
            return 'Slowly progressing fibonacci series x0=%d, x1=%d' % (self.x0, self.x1)

        @process_controller.register(region='region1', wait_sec=t_wait_method_2)
        def state_str_with_side_effect1(self, a, b, c=None, d=None):
            self.state += 1
            return 'State1: %d. Args: %s, %s. Kwargs: c=%s, d=%s' % (self.state, a, b, c, d)

        @process_controller.register('region2', wait_sec=t_wait_method_2)
        def state_str_with_side_effect2(self, a, b, c=None, d=None):
            self.state += 1
            return 'State2: %d. Args: %s, %s. Kwargs: c=%s, d=%s' % (self.state, a, b, c, d)

    #
    # Check that the first fibonacci nums are generated if enough time waited
    #

    # Instantiate 2 objects
    fib1 = FibClass()
    fib2 = FibClass()

    for n in (0, 1, 1, 2, 3, 5):
        first = fib1.next_fibo()
        second = fib1.next_fibo()
        assert first == second == fib1.next_fibo() == n  # but fast calls produce the same number
        if n < 5:
            time.sleep(t_wait_method_1)  # wait enough time for next_fibo to be updated in next call

    #
    # check ProcAction decorator correctly register the methods bound to each instance
    #

    methods1 = process_controller.register.get_registered_by_obj(fib1)
    methods2 = process_controller.register.get_registered_by_obj(fib2)

    for obj, methods in [(fib1, methods1), (fib2, methods2)]:
        print '====== methods of {} ======'.format(obj)
        for met in methods:
            print 'method: {}(), returned: {}'.format(met.__name__, met())

    # methods with same name are not equal because they are bounded to different instances
    assert not filter(None, [(met1 == met2 or met1() == met2()) for met1 in methods1 for met2 in methods2
                             if met1.__name__ == met2.__name__])

    #
    # check we can collect methods from different regions and they don't mix
    #

    methods1 = process_controller.register.get_registered_by_obj(fib1)
    methods2 = process_controller.register.get_registered_by_obj(fib2)
    assert set([m.__name__ for m in methods1]) == set([m.__name__ for m in methods2]) == {'next_fibo', 'message'}

    methods1 = process_controller.register.get_registered_by_obj(fib1, region='region1')
    methods2 = process_controller.register.get_registered_by_obj(fib2, region='region1')
    assert set([m.__name__ for m in methods1]) == set([m.__name__ for m in methods2]) == {'state_str_with_side_effect1'}

    methods1 = process_controller.register.get_registered_by_obj(fib1, region='region2')
    methods2 = process_controller.register.get_registered_by_obj(fib2, region='region2')
    assert set([m.__name__ for m in methods1]) == set([m.__name__ for m in methods2]) == {'state_str_with_side_effect2'}

    #
    # test that calls with different arguments create a new cache value
    #

    state = 0
    r1 = fib1.state_str_with_side_effect1(1, 2, c={3})  # First time call for this method on fib1
    r2 = fib1.state_str_with_side_effect1(1, 2, c={3})  # 2nd time should be cached
    state += 1
    assert r1 == r2 == 'State1: 1. Args: 1, 2. Kwargs: c=set([3]), d=None'

    for arg1 in [1, 2]:
        for d in [None, 'd', [11, 22, 33], {'a': 1, 'b': 2}, lambda x: x]:
            # even d=None will refresh cache, as decorator does not check method's default kwargs
            state += 1
            r3 = fib1.state_str_with_side_effect1(arg1, 2, c={3}, d=d)  # but no cache hit if we change the arguments
            assert r1 != r3 and r3 == 'State1: %s. Args: %s, 2. Kwargs: c=set([3]), d=%s' % (state, arg1, d)

    #
    # Check that we can invalidate cached regions, all methods from objects, and single methods
    #

    state = 0
    r1 = fib2.state_str_with_side_effect1(1, 2, c={69})  # First time call for this method on fib2
    r2 = fib2.state_str_with_side_effect1(1, 2, c={69})  # 2nd time should be cached
    state += 1
    assert r1 == r2 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state

    # wait enough so cache must be refreshed
    time.sleep(t_wait_method_2)
    state += 1
    r3 = fib2.state_str_with_side_effect1(1, 2, c={69})  # this time cache should be refreshed
    assert r2 != r3 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state

    # now lets invalidate the whole cache region and see that it is refreshed
    process_controller.register.invalidate(region='region1')
    r4 = fib2.state_str_with_side_effect1(1, 2, c={69})  # this time cache should be refreshed
    r44 = fib2.state_str_with_side_effect1(1, 2, c={69})  # and here comes from cache again
    state += 1
    assert r3 != r4 == r44 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state

    # now lets invalidate all methods from that object and see that it is refreshed
    process_controller.register.invalidate(region='region1', obj=fib2)
    r5 = fib2.state_str_with_side_effect1(1, 2, c={69})  # this time cache should be refreshed
    r55 = fib2.state_str_with_side_effect1(1, 2, c={69})  # and here comes from cache again
    state += 1
    assert r4 != r5 == r55 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state

    # now lets invalidate only this single method and see that it is refreshed
    process_controller.register.invalidate(region='region1', obj=fib2, method=fib2.state_str_with_side_effect1)
    r6 = fib2.state_str_with_side_effect1(1, 2, c={69})  # this time cache should be refreshed
    r66 = fib2.state_str_with_side_effect1(1, 2, c={69})  # and here comes from cache again
    state += 1
    assert r5 != r6 == r66 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state


class NonSpawningProcessPlus(process_controller.ProcessPlus):
    """
    A Mock Process class to help test process_controller.ProcessPlus without actually spawning new processes
    Ideally we should have patched multiprocessing.Process, but injecting a mock object
    in an inheritance tree is tricky (Composition over Inheritance guys rejoice).
    So it is easier to subclass and make our specialized Mock.
    """

    __created_mocks = 0

    def __init__(self, *args, **kwargs):
        self._testlogger = logging.getLogger(__name__)
        NonSpawningProcessPlus.__created_mocks += 1
        self.id = self.__created_mocks
        self.alive = False
        self.mock_exitcode = None
        super(NonSpawningProcessPlus, self).__init__(*args, **kwargs)
        self.name = '{}-{}'.format(self.__class__.__name__, self.id)
        self._testlogger.info('Mock Process %s instantiated with args=%s, kwargs=%s', self.name, args, kwargs)

    @property
    def pid(self):
        return self.id

    @property
    def exitcode(self):
        return self.mock_exitcode

    def is_alive(self):
        return self.alive

    def start(self):
        self.stats['start_time'] = time.time()
        self.stats['start_time_str'] = self._time2str(self.stats['start_time'])
        self.alive = True
        self._testlogger.info('Mock Process with name=%s, pid=%s started', self.name, self.pid)

    def terminate(self):
        self.alive = False
        self.mock_exitcode = 0
        self._testlogger.info('Mock Process with name=%s, pid=%s terminated', self.name, self.pid)

    @staticmethod
    def reset_mock_counter():
        NonSpawningProcessPlus.__created_mocks = 0


def target(*args, **kwargs):
    assert 0, "target should never be executed by our mocked ProcessPlus"


def test_process_plus_basic(monkeypatch):

    start_timestamp = 1000000000.0
    stop_timestamp = start_timestamp + 100
    middle_timestamp = (start_timestamp + stop_timestamp) / 2.0

    # set time and start the process
    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: start_timestamp)

    pp = NonSpawningProcessPlus(target=target, args=(1, 2), flags=MONITOR_RESTART)
    pp.start()

    assert pp.pid == pp.id  # Mock ProcessPlus class just simulates pid creation

    # Check basic stats keys are present
    stats_keys = ('end_time_str', 'start_time_str', 't_running_secs', 'start_time', 'pid', 'alive', 'rebel', 'name',
                  'stats_closed', 'end_time', 'abnormal_termination', 'exitcode')

    for key in stats_keys:
        assert key in pp.stats

    # set time to some time in the future
    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: middle_timestamp)

    # check that process stats are reported correctly, specially start and stop times
    assert pp.stats['stats_closed'] is False  # stats are not closed until the process is terminated
    assert pp.t_running_secs == middle_timestamp - start_timestamp  # this property always gives the proc running time
    assert pp.stats['t_running_secs'] == 0  # but inside stats the proc running time is not set until termination
    assert pp.stats['start_time'] == start_timestamp
    assert pp.stats['end_time'] is None  # end time is not set until process is terminated
    assert pp.stats['alive'] is True

    # advance more time and terminate the process
    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: stop_timestamp)

    pp.terminate_plus()  # process termination trigger the update of some fields inside stats

    assert pp.stats['stats_closed'] is True  # stats are closed after termination
    assert pp.t_running_secs == stop_timestamp - start_timestamp  # running time property is always right
    assert pp.stats['t_running_secs'] == stop_timestamp - start_timestamp  # running time is now set inside stats
    assert pp.stats['start_time'] == start_timestamp
    assert pp.stats['end_time'] == stop_timestamp  # end time is also set inside stats after termination
    assert pp.stats['alive'] is False

    # Check persisting object to dict works
    exported_fields = ('target', 'args', 'kwargs', 'flags', 'tags', 'stats', 'name', 'pid', 'previous_proc',
                       'ping_status', 'actions_last_5', 'errors_last_5', 'task_counts', 'event_counts')

    assert set(exported_fields) == set(NonSpawningProcessPlus._pack_fields)  # fails if _pack_fields was modified

    dict_repr = pp.to_dict()
    for key in exported_fields:
        assert key in dict_repr

    # Check process sets exitcode on termination
    pp = NonSpawningProcessPlus(target=target, args=(2, 3), flags=MONITOR_RESTART)
    pp.start()
    assert pp.exitcode is None
    pp.terminate_plus()
    assert pp.exitcode == 0

    # terminating a process that is already dead is not an error, but it marks the process as abnormal_termination
    assert pp.abnormal_termination == pp.stats['abnormal_termination'] is False
    pp.terminate_plus()
    assert pp.abnormal_termination == pp.stats['abnormal_termination'] is True

    #
    # Now something fun: recreate a process from a dead one and check proc info is carried on
    #

    pp2 = NonSpawningProcessPlus(**pp.to_dict(serialize_all=True))

    # Notice we passed serialize_all=True, this argument makes the serialization of the target
    # function be performed using the pickle module with protocol 0 (human readable). So it is possible to
    # restart a dead processes in a different process controller process, maybe in another machine.

    assert pp2.target == pp.target  # target function has been pickle and unpickle correctly (see: serialize_all=True)
    assert pp2.args == pp.args
    assert pp2.kwargs == pp.kwargs
    assert pp2.flags == pp.flags

    # the new process retains some data from the process it substituted, makes easier to trace problems
    assert pp2.previous_proc == {
        'dead_name': pp.name,
        'dead_pid': pp.pid,
        'dead_stats': pp.stats,
        'previous_deaths': 0,
    }

    pp2.terminate_plus()

    # check previous_deaths tells you how many times the previous processes was recreated

    pp3 = NonSpawningProcessPlus(**pp2.to_dict(serialize_all=True))
    pp4 = NonSpawningProcessPlus(**pp3.to_dict(serialize_all=True))

    assert pp.previous_proc['previous_deaths'] == -1  # pp was not created from other proc
    assert pp4.previous_proc['previous_deaths'] == 2  # pp3 was recreated 2 times: from pp2 which in turn was from pp

    # assert 0


def test_process_plus_flags(monkeypatch):

    # monkeypatch.setattr('zmon_worker_monitor.process_controller.Process', MockProcess)

    # Start unsupervised process (process controller won't take any action)
    # Pass no flags or flags=MONITOR_NONE
    pp = NonSpawningProcessPlus(target=target, args=(1, 2))
    assert pp.has_flag(MONITOR_NONE) and not (pp.has_flag(MONITOR_RESTART) or pp.has_flag(MONITOR_PING))
    assert pp.is_monitored() is False

    # Start unmonitored process (process controller will restart it if it dies)
    # Pass flags=MONITOR_RESTART
    pp = NonSpawningProcessPlus(target=target, args=(1, 2), flags=MONITOR_RESTART)
    pp.start()  # starting / terminating the process does not alter flags
    pp.terminate_plus()
    assert pp.has_flag(MONITOR_RESTART)
    assert pp.is_monitored() is False

    # Start a fully monitored, supervised process (all actions enabled, this is how we spawn our worker procs).
    # Pass flags=MONITOR_RESTART | MONITOR_PING | MONITOR_KILL_REQ  (flags separated by use bitwise OR, or pass a list)
    # This flags mean process controller will:
    # 1. restart proc if dies (MONITOR_RESTART)
    # 2. collect pings and report tasks done (MONITOR_PING)
    # 3. allow child process to request his own termination via rpc (MONITOR_KILL_REQ)
    pp1 = NonSpawningProcessPlus(target=target, flags=MONITOR_RESTART | MONITOR_PING | MONITOR_KILL_REQ)
    # you can also pass the flags in a list or tuple
    pp2 = NonSpawningProcessPlus(target=target, flags=(MONITOR_RESTART, MONITOR_PING, MONITOR_KILL_REQ))

    assert pp1.has_flag(MONITOR_RESTART) and pp1.has_flag(MONITOR_PING) and pp1.has_flag(MONITOR_KILL_REQ)
    assert pp1.flags == pp2.flags
    assert pp1.is_monitored() == pp2.is_monitored() is True  # processes with MONITOR_PING are monitored

    # assert 0


def test_process_plus_pings(monkeypatch):

    # Deactivate caching. Needed because ping_status calls aggregate_pings() and all aggregations have a short cache
    monkeypatch.setattr('zmon_worker_monitor.process_controller.SimpleMethodCacheInMemory.shortcut_cache', True)

    #
    # Start an unmonitored processes and check ping_status
    #

    pp = NonSpawningProcessPlus(target=target, args=(1, 2), flags=MONITOR_RESTART)
    pp.start()

    assert pp.ping_status == pp.STATUS_NOT_TRACKED  # ping_status is not tracked for unmonitored processes
    pp.terminate_plus()
    assert pp.ping_status == pp.STATUS_BAD_DEAD  # if the process is dead then ping status is BAD-DEAD

    #
    # Start a monitored process and check ping status
    #

    pp = NonSpawningProcessPlus(target=target, args=(2, 3), flags=MONITOR_PING)

    assert pp.is_monitored() is True  # this is a monitored process
    assert pp.ping_status == pp.STATUS_BAD_DEAD  # process is not running yet

    pp.start()

    # ping_status is really calculated only after the process has been running for a certain time,
    # (specified in ProcessPlus.initial_wait_pings), this gives time for some pings to arrive from the child process.
    assert pp.ping_status == pp.STATUS_OK_INITIATING  # process is in initial state as it just started running

    # Lets move time forward and see what ping status will be after the warm up time
    t_real = time.time()

    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_real + pp.initial_wait_pings + 1)

    assert pp.ping_status == pp.STATUS_BAD_NO_PINGS  # time has passed but the process has received no pings

    # provide some fake ping data: this worker has performed 1 task in the last 10 seconds
    ping_worker_ok = {
        'timestamp': t_real + pp.initial_wait_pings / 2.0,
        'timedelta': 10.0,
        'tasks_done': 1,
        'percent_idle': 95.0,
        'task_duration': 3.14,
    }
    pp.add_ping(ping_worker_ok)

    assert pp.ping_status == pp.STATUS_OK  # status of the worker is in good

    # substitute ping data for a fake ping from a worker that has performed no tasks
    ping_worker_idle = {
        'timestamp': t_real + pp.initial_wait_pings / 2.0,
        'timedelta': 10.0,
        'tasks_done': 0,
        'percent_idle': 99.9,
        'task_duration': 3.14,
    }
    pp.stored_pings = [ping_worker_idle]

    assert pp.ping_status == pp.STATUS_OK_IDLE  # worker is in idle state

    # now fake ping data is from a worker that is stuck in a long running task
    ping_worker_idle = {
        'timestamp': t_real + pp.initial_wait_pings / 2.0,
        'timedelta': 10.0,
        'tasks_done': 0,
        'percent_idle': 0.1,
        'task_duration': 3.14,
    }
    pp.stored_pings = [ping_worker_idle]

    assert pp.ping_status == pp.STATUS_WARN_LONG_TASK  # warning worker is running the same task for too long

    monkeypatch.undo()  # undo patching

    #
    # check that we can add well formatted pings and get them back
    #

    # to patch time functions
    t_ping1 = time.time()
    t_ping2 = t_ping1 + 10.0
    t_get_pings = t_ping1 + 20.0

    pp.stored_pings = []  # clear previous data

    ping_data1 = {
        'timestamp': t_ping1,
        'timedelta': 30.0,
        'tasks_done': 5,
        'percent_idle': 15.7,
        'task_duration': 3.14,
    }

    ping_data2 = {
        'timestamp': t_ping2,
        'timedelta': 30.0,
        'tasks_done': 1,
        'percent_idle': 91.2,
        'task_duration': 3.14,
    }

    # lets add 2 pings
    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_ping1)
    pp.add_ping(ping_data1)

    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_ping2)
    pp.add_ping(ping_data2)

    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_get_pings)

    # the ping data can be retrieved
    assert [ping_data1, ping_data2] == pp.get_pings()
    assert [ping_data2] == pp.get_pings(limit=1)  # limit to last ping only
    assert [ping_data2] == pp.get_pings(interval=11)  # filter by interval get last ping only
    assert [ping_data2] == pp.get_pings(interval=11, limit=1)  # filter by interval and limit
    assert [ping_data1, ping_data2] == pp.get_pings(interval=21, limit=2)

    #
    # check that we can add well formatted events and can get them back
    #

    # to patch time functions
    t_event1 = time.time()
    t_event2 = t_event1 + 10.0
    t_get_events = t_event1 + 20.0

    pp.stored_events = []  # clear previous data

    event_data1 = {
        'origin': 'test_process_controller.test_process_plus_pings',
        'type': 'ACTION',
        'body': 'whatever',
        'timestamp': t_event1,
        'repeats': 1,
    }

    event_data2 = {
        'origin': 'test_process_controller.test_process_plus_pings',
        'type': 'ERROR',
        'body': 'whatever',
        'timestamp': t_event2,
        'repeats': 10,
    }

    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_event1)
    pp.add_event(event_data1)

    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_event2)
    # this event we add using our explicit method, timestamp will be set by the receiver
    pp.add_event_explicit(event_data2['origin'], event_data2['type'], event_data2['body'],
                          repeats=event_data2['repeats'])

    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_get_events)

    # the event data can be retrieved
    assert [event_data1, event_data2] == pp.get_events()
    assert [event_data2] == pp.get_events(limit=1)  # limit to last event only

    assert [event_data2] == pp.get_events(interval=11)    # get last event filtering by time interval
    assert [event_data1, event_data2] == pp.get_events(interval=21)  # get all events filtering by time interval

    assert [event_data2] == pp.get_events(event_type='ERROR', interval=11)  # filter by event_type and interval
    assert [] == pp.get_events(event_type='ACTION', interval=11)  # filter by event_type and interval

    #
    # check that adding a bad formatted pings or events raise the error we expect
    #

    for data in (None, object, 123, {'any_key': 123}):

        with pytest.raises(AssertionError):
            pp.add_ping(data)

        with pytest.raises(AssertionError):
            pp.add_event(data)

    # check that event types different from ACTION or ERROR is rejected
    event_data3 = event_data1.copy()
    event_data3['type'] = 'SOME_INVALID_EVENT_TYPE'

    with pytest.raises(AssertionError):
        pp.add_event(event_data3)

    # check that events with repeats < 1 are rejected
    event_data4 = event_data1.copy()
    event_data4['repeats'] = 0

    with pytest.raises(AssertionError):
        pp.add_event(event_data4)

    monkeypatch.undo()  # undo patching

    # assert 0


def test_process_plus_ping_aggregations(monkeypatch):

    # Deactivate caching. Needed because all aggregation functions have a short cache
    monkeypatch.setattr('zmon_worker_monitor.process_controller.SimpleMethodCacheInMemory.shortcut_cache', True)

    pp = NonSpawningProcessPlus(target=target, args=(1, 2), flags=MONITOR_PING)

    pp.start()

    # check ping aggretation with no ping data
    aggregated_pings_no_data = {'tasks_per_sec': -1, 'tasks_per_min': -1, 'percent_idle': -1, 'interval': 0,
                                'tasks_done': -1, 'pings_received': -1, 'average_task_duration': -1}

    assert pp.aggregate_pings() == aggregated_pings_no_data

    # add a number of pings spaced with timestamps every 30 seconds and check the aggregation and
    # to_dict()['task_counts']['0:05:00']

    # to patch time functions
    ping_delta = 30  # pings will be spaced by 30 seconds
    num_pings = 3
    t_ping_start = time.time()
    t_ping_end = t_ping_start + ping_delta * (num_pings - 1)  # pings will be spaced by ping_delta secs
    t_get_aggregation = t_ping_end + 5

    # simulate num_pings pings sent every ping_delta seconds
    pings_sample = [
        {
            'timestamp': t_ping_start + i * ping_delta,
            'timedelta': ping_delta,
            'tasks_done': 1,
            'percent_idle': 85.0,
            'task_duration': 3.14,
        }
        for i in range(num_pings)
    ]

    for ping_data in pings_sample:
        monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: ping_data['timestamp'])
        pp.add_ping(ping_data)

    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_get_aggregation)

    # calculate the aggregations for our sample data
    interval = t_get_aggregation - t_ping_start  # if interval is not passed the aggregation goes from the first ping
    sum_tasks = sum([p['tasks_done'] for p in pings_sample])
    tasks_per_sec = (sum_tasks * 1.0) / interval

    aggregated_results = {
        'tasks_per_sec': tasks_per_sec,
        'tasks_per_min': tasks_per_sec * 60,
        'percent_idle': 85.0,
        'interval': interval,
        'tasks_done': sum_tasks,
        'pings_received': len(pings_sample),
        'average_task_duration': sum([p['task_duration'] for p in pings_sample]) / sum_tasks,
    }

    # Because of rounding we need to compare the dicts values differ in less than a delta
    def compare_float_values(d1, d2, delta=0.0001):
        return set(d1.keys()) == set(d2.keys()) and not [k for k in d1 if abs(d2[k] - d1[k]) > delta]

    assert compare_float_values(pp.aggregate_pings(), aggregated_results)

    # let's try now the aggregation passing an interval that includes only the last ping
    interval = ping_delta + 1
    tasks_per_sec = (pings_sample[-1]['tasks_done'] * 1.0) / interval

    aggregated_results_last_ping = {
        'tasks_per_sec': tasks_per_sec,
        'tasks_per_min': tasks_per_sec * 60,
        'percent_idle': 85.0,
        'interval': interval,
        'tasks_done': pings_sample[-1]['tasks_done'],
        'pings_received': 1,
        'average_task_duration': pings_sample[-1]['task_duration'] / pings_sample[-1]['tasks_done'],
    }

    assert compare_float_values(pp.aggregate_pings(interval=interval), aggregated_results_last_ping)

    # lets now check that the dict representation export the same ping aggregation results under task_counts key
    dict_repr = pp.to_dict()

    # one of the aggregations persisted under task_counts is done with 5 minutes interval
    # notice task_counts is a dict that put aggregations under a human readable key
    # that human readable key format comes from str(datetime.timedelta(seconds=x))
    assert compare_float_values(pp.aggregate_pings(interval=300), dict_repr['task_counts']['0:05:00'])

    # check all persisted aggregations
    for k, v in dict_repr['task_counts'].items():
        assert compare_float_values(pp.aggregate_pings(interval=v['interval']), v)

    pp.terminate_plus()

    monkeypatch.undo()


def test_process_plus_event_aggregations(monkeypatch):

    # Deactivate caching. Needed because all aggregation functions have a short cache
    monkeypatch.setattr('zmon_worker_monitor.process_controller.SimpleMethodCacheInMemory.shortcut_cache', True)

    pp = NonSpawningProcessPlus(target=target, args=(1, 2), flags=MONITOR_PING)

    pp.start()

    # check event aggretation with no ping data
    aggregated_events_no_data = {
        'interval': 0,
        'totals': {
            'events': 0,
            'actions': 0,
            'errors': 0,
        },
        'by_origin': {
            'events': {},
            'actions': {},
            'errors': {},
        },
    }

    assert pp.aggregate_events() == aggregated_events_no_data

    # add a number of events spaced with timestamps every 30 seconds and check the aggregation and
    # to_dict()['event_counts']['1 day, 0:00:00']

    # to patch time functions
    event_delta = 30  # pings will be spaced by 30 seconds
    num_events = 10
    t_event_start = time.time()
    t_event_end = t_event_start + event_delta * (num_events - 1)  # pings will be spaced by ping_delta secs
    t_get_aggregation = t_event_end + 5

    def fake_repeats(i):
        return 1 if i != 1 else 5

    # simulate num_events events sent every event_delta seconds
    events_sample = [
        {
            'origin': 'ori.' + str(i),
            'type': ['ACTION', 'ERROR'][i % 2],
            'body': 'body.{}'.format(i),
            'timestamp': t_event_start + i * event_delta,
            'repeats': fake_repeats(i),  # all set to 1, except for i==1 where we set 5
        }
        for i in range(num_events)
    ]

    for event_data in events_sample:
        monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: event_data['timestamp'])
        pp.add_event(event_data)

    monkeypatch.setattr('zmon_worker_monitor.process_controller.time.time', lambda: t_get_aggregation)

    # calculate the aggregations for our sample data
    interval = t_get_aggregation - t_event_start  # if interval is not passed the aggregation goes from the first ping
    sum_events = sum([e['repeats'] for e in events_sample])
    sum_actions = sum([e['repeats'] for e in events_sample if e['type'] == 'ACTION'])
    sum_errors = sum([e['repeats'] for e in events_sample if e['type'] == 'ERROR'])

    # there must be one per origin (we mutate origin all the time), but remember i == 1 has repeats=5 (fake_repeats)
    events_by_origin = {'ori.' + str(i): fake_repeats(i) for i, e in enumerate(events_sample)}
    actions_by_origin = {'ori.' + str(i): fake_repeats(i) for i, e in enumerate(events_sample) if e['type'] == 'ACTION'}
    error_by_origin = {'ori.' + str(i): fake_repeats(i) for i, e in enumerate(events_sample) if e['type'] == 'ERROR'}

    aggregated_results = {
        'interval': interval,
        'totals': {
            'events': sum_events,
            'actions': sum_actions,
            'errors': sum_errors,
        },
        'by_origin': {
            'events': events_by_origin,
            'actions': actions_by_origin,
            'errors': error_by_origin,
        },
    }

    assert pp.aggregate_events() == aggregated_results

    # let's try now the aggregation passing an interval that includes only the last event
    events_sample[-1]['type'] = 'ERROR'  # force the last one to be an ERROR

    last_index = len(events_sample) - 1

    interval = event_delta + 1

    aggregated_events_last = {
        'interval': interval,
        'totals': {
            'events': 1,
            'actions': 0,
            'errors': 1,
        },
        'by_origin': {
            'events': {'ori.{}'.format(last_index): fake_repeats(last_index)},
            'actions': {},
            'errors': {'ori.{}'.format(last_index): fake_repeats(last_index)},
        },
    }

    assert pp.aggregate_events(interval=interval) == aggregated_events_last

    # lets now check that the dict representation export the same ping aggregation results under task_counts key
    dict_repr = pp.to_dict()

    # one of the aggregations persisted under event_counts is done with 1 day interval
    # notice event_counts is a dict that put aggregations under a human readable key
    # that human readable key format comes from str(datetime.timedelta(seconds=x))
    assert pp.aggregate_events(interval=60 * 60 * 24) == dict_repr['event_counts']['1 day, 0:00:00']

    # check all persisted aggregations
    for k, v in dict_repr['event_counts'].items():
        assert pp.aggregate_events(interval=v['interval']) == v

    monkeypatch.undo()

    # assert 0


def test_process_group(monkeypatch):

    # Deactivate cache. Actions are decorated with register(), which is a reference of our cache decorator
    monkeypatch.setattr('zmon_worker_monitor.process_controller.SimpleMethodCacheInMemory.shortcut_cache', True)

    # Reset the Mock counter, this test rely on mock generation counter starting from 1
    NonSpawningProcessPlus.reset_mock_counter()

    action_interval = 0.2
    num_procs = 3

    # create our process_group, a dict like object mapping proc_name -> objProcessPlus
    pg = process_controller.ProcessGroup(group_name='main', process_plus_impl=NonSpawningProcessPlus)

    # start action loop to supervise and monitor
    pg.start_action_loop(interval=action_interval)

    # group has no running processes yet
    assert len(pg) == pg.total_processes() == 0

    # spawn a num_procs-1 processes with monitoring and supervision capabilities (this is how we start the workers)
    pg.spawn_many(num_procs - 1, target=target, args=(1, 2), kwargs={"a": 1, "b": 2},
                  flags=MONITOR_RESTART | MONITOR_KILL_REQ | MONITOR_PING)

    # spawn 1 last process only with restart supervision capability (this is how we start the web server)
    pg.spawn_process(target=target, args=(3, 4), flags=MONITOR_RESTART)

    # now the group should have num_procs processes running
    assert len(pg) == pg.total_processes() == num_procs

    # we recognize all processes are monitored for pings except the last one
    for i, p_name in enumerate(sorted(pg)):
        assert pg[p_name].is_monitored() == (True if i < len(pg) - 1 else False)

    # lets kill the first process, that should be named NonSpawningProcessPlus-1
    proc_name = 'NonSpawningProcessPlus-1'
    proc_ref = pg[proc_name]  # keep a reference to be able to use it later
    proc_name_next = 'NonSpawningProcessPlus-{}'.format(num_procs + 1)  # this is not alive yet

    assert pg[proc_name].is_alive() is True  # proc is alive

    assert proc_name_next not in pg  # not spawned, it was out of the range of specified num_procs

    # kill selected process directly (without informing the group)
    pg[proc_name].terminate()

    assert pg[proc_name].is_alive() is False  # process is immediately dead
    # action has not kicked in yet
    assert len(pg) == pg.total_processes() == 3
    assert len(pg.dead_group) == pg.total_dead_processes() == 0

    time.sleep(action_interval * 2)  # wait enough time for the actions to kick in

    # now the action loop moved the dead process to dead_group and a new process took its place
    assert len(pg) == pg.total_processes() == 3
    assert len(pg.dead_group) == pg.total_dead_processes() == 1

    # check the proc in dead_group is really the one we killed
    assert pg.dead_group[proc_name] == proc_ref

    # check we filled in correctly the previous_proc info in the new proc
    assert proc_name_next in pg and pg[proc_name_next].previous_proc['dead_name'] == proc_ref.name and \
        pg[proc_name_next].previous_proc['dead_pid'] == proc_ref.pid

    # stop all processes
    pg.stop_action_loop()
    pg.terminate_all()

    assert len(pg) == pg.total_processes() == 0


def test_process_group_health(monkeypatch):

    # Deactivate cache. Actions are decorated with register(), which is a reference of our cache decorator
    monkeypatch.setattr('zmon_worker_monitor.process_controller.SimpleMethodCacheInMemory.shortcut_cache', True)

    # Reset the Mock counter, this test rely on mock generation counter starting from 1
    NonSpawningProcessPlus.reset_mock_counter()

    action_interval = 0.2
    num_procs = 4

    # create our process_group, a dict like object mapping proc_name -> objProcessPlus
    pg = process_controller.ProcessGroup(group_name='main', process_plus_impl=NonSpawningProcessPlus)

    # spawn a num_procs processes with monitoring and supervision capabilities
    pg.spawn_many(num_procs, target=target, args=(1, 2), kwargs={"a": 1, "b": 2},
                  flags=MONITOR_RESTART | MONITOR_KILL_REQ | MONITOR_PING)

    # kill half of the processes directly (without informing the group)
    names_to_kill = [p_name for i, p_name in enumerate(sorted(pg.keys())) if i % 2 == 0]

    for name in names_to_kill:
        pg[name].terminate()

    # now the health of the group is compromised
    assert pg.is_healthy() is False

    # start action loop to supervise and monitor
    pg.start_action_loop(interval=action_interval)

    time.sleep(action_interval * 2)  # wait enough time for the actions to finish

    # check the health status of the process group
    assert pg.is_healthy() is True

    #
    # check process_view is correct
    #

    process_view = pg.processes_view()

    assert len(process_view['running']) == num_procs
    assert len(process_view['dead']) == len(names_to_kill)

    assert set([d['name'] for d in process_view['dead']]) == set(names_to_kill)

    # check the ping_status is reported correctly
    for p in process_view['dead']:
        # check events are reported in the process: a supervised process death is an event of type ACTION, not ERROR
        assert len(p['actions_last_5']) == 1 and len(p['errors_last_5']) == 0

        # check that the action event is correct
        action = p['actions_last_5'][0]
        assert action['type'] == 'ACTION' and action['repeats'] == 1
        assert action['origin'].endswith('_action_restart_dead')
        assert action['body'].startswith('Detected abnormal termination')

    #
    # check status_view is correct
    #

    status_view = pg.status_view()

    assert status_view['events']['num_events'] == status_view['events']['num_actions'] == len(names_to_kill)
    assert status_view['events']['num_errors'] == 0

    assert status_view['totals']['total_dead_processes'] == len(names_to_kill)
    assert status_view['totals']['total_processes'] == num_procs
    assert status_view['totals']['total_monitored_processes'] == num_procs

    # the process are not reported as idle because they are in the warm up time
    assert status_view['idle']['idle_procs'] == [] and status_view['idle']['num_idle_procs'] == 0

    # stop all processes
    pg.stop_action_loop()
    pg.terminate_all()

    assert len(pg) == pg.total_processes() == 0
